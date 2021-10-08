package model

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"

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
