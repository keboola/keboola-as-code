package model

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// PathsState keeps state of all files/dirs in projectDir.
type PathsState struct {
	fs      filesystem.Fs
	all     map[string]bool
	tracked map[string]bool
	isFile  map[string]bool
}

type PathState int

const (
	Tracked PathState = iota
	Untracked
	Ignored
)

func NewPathsState(fs filesystem.Fs) (*PathsState, error) {
	f := &PathsState{
		fs:      fs,
		all:     make(map[string]bool),
		tracked: make(map[string]bool),
		isFile:  make(map[string]bool),
	}
	err := f.init()
	return f, err
}

func (f *PathsState) Clone() *PathsState {
	n := &PathsState{
		fs:      f.fs,
		all:     make(map[string]bool),
		tracked: make(map[string]bool),
		isFile:  make(map[string]bool),
	}

	// Copy all fields
	for k, v := range f.all {
		n.all[k] = v
	}
	for k, v := range f.tracked {
		n.tracked[k] = v
	}
	for k, v := range f.isFile {
		n.isFile[k] = v
	}
	return n
}

// State returns state of path.
func (f *PathsState) State(path string) PathState {
	if _, ok := f.tracked[path]; ok {
		return Tracked
	}
	if _, ok := f.all[path]; ok {
		return Untracked
	}
	return Ignored
}

func (f *PathsState) IsTracked(path string) bool {
	return f.State(path) == Tracked
}

func (f *PathsState) IsUntracked(path string) bool {
	return f.State(path) == Untracked
}

// TrackedPaths returns all tracked paths.
func (f *PathsState) TrackedPaths() []string {
	var tracked []string
	for path := range f.tracked {
		tracked = append(tracked, path)
	}
	sort.Strings(tracked)
	return tracked
}

// UntrackedPaths returns all untracked paths.
func (f *PathsState) UntrackedPaths() []string {
	var untracked []string
	for path := range f.all {
		if _, ok := f.tracked[path]; !ok {
			untracked = append(untracked, path)
		}
	}
	sort.Strings(untracked)
	return untracked
}

func (f *PathsState) UntrackedDirs() (dirs []string) {
	for _, path := range f.UntrackedPaths() {
		if !f.fs.IsDir(path) {
			continue
		}
		dirs = append(dirs, path)
	}
	return dirs
}

func (f *PathsState) UntrackedDirsFrom(base string) (dirs []string) {
	for _, path := range f.UntrackedPaths() {
		if !f.fs.IsDir(path) || !filesystem.IsFrom(path, base) {
			continue
		}
		dirs = append(dirs, path)
	}
	return dirs
}

func (f *PathsState) LogUntrackedPaths(logger *zap.SugaredLogger) {
	untracked := f.UntrackedPaths()
	if len(untracked) > 0 {
		logger.Warn("Unknown paths found:")
		for _, path := range untracked {
			logger.Warn("\t- ", path)
		}
	}
}

func (f *PathsState) IsFile(path string) bool {
	v, ok := f.isFile[path]
	if !ok {
		panic(fmt.Errorf(`unknown path "%s"`, path))
	}
	return v
}

func (f *PathsState) IsDir(path string) bool {
	return !f.IsFile(path)
}

// MarkTracked path and all parent paths.
func (f *PathsState) MarkTracked(path string) {
	// Add path and all parents
	for {
		// Is path known (not ignored)?
		if _, ok := f.all[path]; !ok {
			return
		}

		// Mark
		f.tracked[path] = true

		// Process parent path
		path = filesystem.Dir(path)
	}
}

// MarkSubPathsTracked path and all parent and sub paths.
func (f *PathsState) MarkSubPathsTracked(path string) {
	f.MarkTracked(path)

	// Mark tracked all sub paths
	for subPath := range f.all {
		if filesystem.IsFrom(subPath, path) {
			f.tracked[subPath] = true
		}
	}
}

func (f *PathsState) init() error {
	errors := utils.NewMultiError()
	root := "."
	err := f.fs.Walk(root, func(path string, info fs.FileInfo, err error) error {
		// Log error
		if err != nil {
			errors.Append(err)
			return nil
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Is ignored?
		if f.isIgnored(path) {
			if info.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		f.all[path] = true
		f.isFile[path] = f.fs.IsFile(path)
		return nil
	})
	// Errors are not critical, they can be e.g. problem with permissions
	if err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

func (f *PathsState) isIgnored(path string) bool {
	// Ignore empty and hidden paths
	return path == "" || path == "." || strings.HasPrefix(filesystem.Base(path), ".")
}
