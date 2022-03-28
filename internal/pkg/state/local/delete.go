package local

import (
	"io/fs"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type deleteCtx struct {
	*uow
	key       model.Key
	onSuccess func()
}

func (c *deleteCtx) delete() {
	c.
		workersFor(c.key.Level()).
		AddWorker(func() error {
			// Remove manifest record
			c.state.manifest.Remove(c.key)

			// Remove all related files
			errors := utils.NewMultiError()
			for _, path := range c.state.getRelatedPaths(c.key).All() {
				if c.state.fs.IsFile(path) {
					if err := c.state.fs.Remove(path); err != nil {
						errors.Append(err)
					}
				}
			}

			if errors.Len() == 0 {
				c.onSuccess()
			}

			return errors.ErrorOrNil()
		})
}

func deleteEmptyDirectories(fs filesystem.Fs, knowPaths *knownpaths.Paths) error {
	errors := utils.NewMultiError()
	emptyDirs := orderedmap.New()
	root := `.`
	err := fs.Walk(root, func(path string, info filesystem.FileInfo, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Stop on ignored dir
		skipDir := isIgnoredDir(path, info)

		// Found a directory -> store path
		if !skipDir && info.IsDir() {
			emptyDirs.Set(path, true)
			return nil
		}

		// Found file/ignored dir -> all parent dirs are not empty
		for _, dir := range emptyDirs.Keys() {
			if filesystem.IsFrom(path, dir) {
				emptyDirs.Delete(dir)
			}
		}

		// Skip sub-directories
		if skipDir {
			return filesystem.SkipDir
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Sort by the longest path first -> delete most nested directory first
	emptyDirs.SortKeys(func(keys []string) {
		sort.SliceStable(keys, func(i, j int) bool {
			return len(keys[i]) > len(keys[j])
		})
	})

	// Remove only empty dirs, if parent dir is from a tracked dirs
	dirsToRemove := make([]string, 0)
	for _, dir := range emptyDirs.Keys() {
		for _, tracked := range knowPaths.TrackedPaths() {
			if tracked == dir || filesystem.IsFrom(dir, tracked) {
				// Remove dir, it is from a tracked dir
				dirsToRemove = append(dirsToRemove, dir)
				break
			}
		}
	}

	// Delete
	for _, dir := range dirsToRemove {
		if err := fs.Remove(dir); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func isIgnoredDir(path string, info fs.FileInfo) bool {
	base := filesystem.Base(path)
	return info.IsDir() && strings.HasPrefix(base, ".")
}
