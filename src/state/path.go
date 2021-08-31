package state

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"keboola-as-code/src/utils"
)

// PathsState keeps state of all files/dirs in projectDir.
type PathsState struct {
	error      *utils.Error
	projectDir string
	all        map[string]bool
	tracked    map[string]bool
}

type PathState int

const (
	Tracked PathState = iota
	Untracked
	Ignored
)

func NewPathsState(projectDir string, error *utils.Error) *PathsState {
	if !utils.IsDir(projectDir) {
		panic(fmt.Errorf("directory \"%s\" not found", projectDir))
	}

	f := &PathsState{
		error:      error,
		projectDir: projectDir,
		all:        make(map[string]bool),
		tracked:    make(map[string]bool),
	}
	f.init()
	return f
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

// Tracked returns all tracked paths.
func (f *PathsState) Tracked() []string {
	var tracked []string
	for path := range f.tracked {
		tracked = append(tracked, path)
	}
	sort.Strings(tracked)
	return tracked
}

// Untracked returns all untracked paths.
func (f *PathsState) Untracked() []string {
	var untracked []string
	for path := range f.all {
		if _, ok := f.tracked[path]; !ok {
			untracked = append(untracked, path)
		}
	}
	sort.Strings(untracked)
	return untracked
}

func (f *PathsState) MarkTracked(path string) {
	path = f.relative(path)

	// Add path and all parents
	for {
		// Is path known (not ignored)?
		if _, ok := f.all[path]; !ok {
			return
		}

		// Mark
		f.tracked[path] = true

		// Process parent path
		path = filepath.Dir(path)
	}
}

func (f *PathsState) init() {
	err := filepath.WalkDir(f.projectDir, func(path string, d fs.DirEntry, err error) error {
		// Log error
		if err != nil {
			f.error.Append(err)
			return nil
		}

		// Ignore root
		if path == f.projectDir {
			return nil
		}

		// Is ignored?
		if f.isIgnored(path) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		path = f.relative(path)
		f.all[path] = true
		return nil
	})

	// Errors are not critical, they can be e.g. problem with permissions
	if err != nil {
		f.error.Append(err)
	}
}

func (f *PathsState) isIgnored(path string) bool {
	// Ignore empty and hidden paths
	return path == "" || path == "." || strings.HasPrefix(filepath.Base(path), ".")
}

func (f *PathsState) relative(path string) string {
	if !filepath.IsAbs(path) {
		return path
	}

	if !strings.HasPrefix(path, f.projectDir+string(filepath.Separator)) {
		panic(fmt.Errorf("path \"%s\" is not from the project dir \"%s\"", path, f.projectDir))
	}

	return utils.RelPath(f.projectDir, path)
}
