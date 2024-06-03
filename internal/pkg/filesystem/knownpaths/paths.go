package knownpaths

import (
	"context"
	"io/fs"
	"sort"
	"strings"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Paths keeps state of all files/dirs in projectDir.
type Paths struct {
	lock    *sync.Mutex
	fs      filesystem.Fs
	filter  IsIgnoredFn
	all     map[string]bool
	tracked map[string]bool
	isFile  map[string]bool
}

type Option func(p *Paths)

type IsIgnoredFn func(ctx context.Context, path string) (bool, error)

func WithFilter(fn IsIgnoredFn) Option {
	return func(p *Paths) {
		p.filter = fn
	}
}

type PathState int

const (
	Tracked PathState = iota
	Untracked
	Ignored
)

func New(ctx context.Context, fs filesystem.Fs, options ...Option) (*Paths, error) {
	v := &Paths{
		lock: &sync.Mutex{},
		fs:   fs,
	}

	// Apply options
	for _, o := range options {
		o(v)
	}

	err := v.init(ctx)
	return v, err
}

func NewNop(ctx context.Context) *Paths {
	paths, err := New(ctx, aferofs.NewMemoryFs())
	if err != nil {
		panic(err)
	}
	return paths
}

func (p *Paths) Reset(ctx context.Context) error {
	if err := p.init(ctx); err != nil {
		return err
	}
	return nil
}

func (p *Paths) ReadOnly() *PathsReadOnly {
	return &PathsReadOnly{paths: p}
}

func (p *Paths) Clone() *Paths {
	p.lock.Lock()
	defer p.lock.Unlock()

	n := &Paths{
		lock:    &sync.Mutex{},
		fs:      p.fs,
		all:     make(map[string]bool),
		tracked: make(map[string]bool),
		isFile:  make(map[string]bool),
	}

	// Copy all fields
	for k, v := range p.all {
		n.all[k] = v
	}
	for k, v := range p.tracked {
		n.tracked[k] = v
	}
	for k, v := range p.isFile {
		n.isFile[k] = v
	}
	return n
}

// State returns state of path.
func (p *Paths) State(path string) PathState {
	p.lock.Lock()
	defer p.lock.Unlock()

	if _, ok := p.tracked[path]; ok {
		return Tracked
	}
	if _, ok := p.all[path]; ok {
		return Untracked
	}
	return Ignored
}

func (p *Paths) IsTracked(path string) bool {
	return p.State(path) == Tracked
}

func (p *Paths) IsUntracked(path string) bool {
	return p.State(path) == Untracked
}

// TrackedPaths returns all tracked paths.
func (p *Paths) TrackedPaths() []string {
	p.lock.Lock()
	defer p.lock.Unlock()

	tracked := make([]string, 0, len(p.tracked))
	for path := range p.tracked {
		tracked = append(tracked, path)
	}
	sort.Strings(tracked)
	return tracked
}

// UntrackedPaths returns all untracked paths.
func (p *Paths) UntrackedPaths() []string {
	p.lock.Lock()
	defer p.lock.Unlock()

	untracked := make([]string, 0, len(p.all))
	for path := range p.all {
		if _, ok := p.tracked[path]; !ok {
			untracked = append(untracked, path)
		}
	}
	sort.Strings(untracked)
	return untracked
}

func (p *Paths) UntrackedDirs(ctx context.Context) (dirs []string) {
	for _, path := range p.UntrackedPaths() {
		if !p.fs.IsDir(ctx, path) {
			continue
		}
		dirs = append(dirs, path)
	}
	return dirs
}

func (p *Paths) UntrackedDirsFrom(ctx context.Context, base string) (dirs []string) {
	for _, path := range p.UntrackedPaths() {
		if !p.fs.IsDir(ctx, path) || !filesystem.IsFrom(path, base) {
			continue
		}
		dirs = append(dirs, path)
	}
	return dirs
}

func (p *Paths) LogUntrackedPaths(ctx context.Context, logger log.Logger) {
	untracked := p.UntrackedPaths()
	if len(untracked) > 0 {
		logger.Warn(ctx, "Unknown paths found:")
		for _, path := range untracked {
			logger.Warnf(ctx, "  - %s", path)
		}
	}
}

func (p *Paths) IsFile(path string) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	v, ok := p.isFile[path]
	if !ok {
		panic(errors.Errorf(`unknown path "%s"`, path))
	}
	return v
}

func (p *Paths) IsDir(path string) bool {
	return !p.IsFile(path)
}

// MarkTracked path and all parent paths.
func (p *Paths) MarkTracked(path string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Add path and all parents
	for {
		// Is path known (not ignored)?
		if _, ok := p.all[path]; ok {
			// Mark
			p.tracked[path] = true
		}

		// Process parent path
		path = filesystem.Dir(path)
		if path == "." {
			break
		}
	}
}

// MarkSubPathsTracked path and all parent and sub paths.
func (p *Paths) MarkSubPathsTracked(path string) {
	p.MarkTracked(path)

	// Mark tracked all sub paths
	p.lock.Lock()
	defer p.lock.Unlock()
	for subPath := range p.all {
		if filesystem.IsFrom(subPath, path) {
			p.tracked[subPath] = true
		}
	}
}

func (p *Paths) init(ctx context.Context) error {
	p.all = make(map[string]bool)
	p.tracked = make(map[string]bool)
	p.isFile = make(map[string]bool)

	errs := errors.NewMultiError()
	root := "."
	err := p.fs.Walk(ctx, root, func(path string, info fs.FileInfo, err error) error {
		// Log error
		if err != nil {
			errs.Append(err)
			return nil
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Is ignored?
		if ignored, err := p.isIgnored(ctx, path); err != nil {
			return err
		} else if ignored {
			if info.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		p.all[path] = true
		p.isFile[path] = p.fs.IsFile(ctx, path)
		return nil
	})
	// Errors are not critical, they can be e.g. problem with permissions
	if err != nil {
		errs.Append(err)
	}

	return errs.ErrorOrNil()
}

func (p *Paths) isIgnored(ctx context.Context, path string) (bool, error) {
	// Ignore empty and hidden paths
	if path == "" || path == "." || strings.HasPrefix(filesystem.Base(path), ".") {
		return true, nil
	}

	// Use filter, if it is set
	if p.filter != nil {
		return p.filter(ctx, path)
	}

	return false, nil
}

type PathsReadOnly struct {
	paths *Paths
}

func (p *PathsReadOnly) KnownPaths() *Paths {
	return p.paths.Clone()
}

func (p *PathsReadOnly) IsTracked(path string) bool {
	return p.paths.IsTracked(path)
}

func (p *PathsReadOnly) IsUntracked(path string) bool {
	return p.paths.IsUntracked(path)
}

// TrackedPaths returns all tracked paths.
func (p *PathsReadOnly) TrackedPaths() []string {
	return p.paths.TrackedPaths()
}

// UntrackedPaths returns all untracked paths.
func (p *PathsReadOnly) UntrackedPaths() []string {
	return p.paths.UntrackedPaths()
}

func (p *PathsReadOnly) UntrackedDirs() (dirs []string) {
	return p.paths.UntrackedPaths()
}

func (p *PathsReadOnly) UntrackedDirsFrom(ctx context.Context, base string) (dirs []string) {
	return p.paths.UntrackedDirsFrom(ctx, base)
}

func (p *PathsReadOnly) IsFile(path string) bool {
	return p.paths.IsFile(path)
}

func (p *PathsReadOnly) IsDir(path string) bool {
	return p.paths.IsDir(path)
}

func (p *PathsReadOnly) LogUntrackedPaths(ctx context.Context, logger log.Logger) {
	p.paths.LogUntrackedPaths(ctx, logger)
}
