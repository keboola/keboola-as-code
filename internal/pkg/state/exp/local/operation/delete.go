package operation

import (
	"io/fs"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// DeleteObject from manifest and filesystem.
func (m *Manager) DeleteObject(key model.Key) error {
	// Get record
	objectManifest, found := m.manifest.GetRecord(key)
	if !found {
		return nil
	}

	// Remove manifest record
	m.manifest.Remove(objectManifest)

	// Remove all related files
	errors := utils.NewMultiError()
	for _, path := range objectManifest.GetRelatedPaths() {
		if m.fs.IsFile(path) {
			if err := m.fs.Remove(path); err != nil {
				errors.Append(err)
			}
		}
	}

	return errors.ErrorOrNil()
}

// DeleteInvalidObjects from disk, eg. if pull --force used.
func (m *Manager) DeleteInvalidObjects() error {
	errors := utils.NewMultiError()
	for _, objectManifest := range m.manifest.All() {
		if objectManifest.State().IsInvalid() {
			if err := m.DeleteObject(objectManifest); err != nil {
				errors.Append(err)
			}
		}
	}
	return errors.ErrorOrNil()
}

// DeleteEmptyDirectories from project directory (eg. dir with extractors, but no extractor left)
// Deleted are only empty directories from know/tracked paths.
// Hidden dirs are ignored.
func (m *Manager) DeleteEmptyDirectories() error {
	errors := utils.NewMultiError()
	emptyDirs := orderedmap.New()
	root := `.`
	err := m.fs.Walk(root, func(path string, info filesystem.FileInfo, err error) error {
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
		for _, tracked := range trackedPaths {
			if tracked == dir || filesystem.IsFrom(dir, tracked) {
				// Remove dir, it is from a tracked dir
				dirsToRemove = append(dirsToRemove, dir)
				break
			}
		}
	}

	// Delete
	for _, dir := range dirsToRemove {
		if err := m.fs.Remove(dir); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func isIgnoredDir(path string, info fs.FileInfo) bool {
	base := filesystem.Base(path)
	return info.IsDir() && strings.HasPrefix(base, ".")
}
