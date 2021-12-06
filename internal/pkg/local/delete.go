package local

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// DeleteInvalidObjects from disk, eg. if pull --force used.
func (m *Manager) DeleteInvalidObjects() error {
	errors := utils.NewMultiError()
	records := m.manifest.GetRecords()
	for _, key := range append([]string(nil), records.Keys()...) {
		v, _ := records.Get(key)
		manifest := v.(model.ObjectManifest)
		if manifest.State().IsInvalid() {
			if err := m.deleteObject(manifest); err != nil {
				errors.Append(err)
			}
		}
	}
	return errors.ErrorOrNil()
}

// DeleteEmptyDirectories from project directory (eg. dir with extractors, but no extractor left)
// Deleted are only empty directories from know/tracked paths.
// Hidden dirs are ignored.
func DeleteEmptyDirectories(fs filesystem.Fs, trackedPaths []string) error {
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

	// Sort by the longest path firs -> delete most nested directory first
	emptyDirs.SortKeys(func(keys []string) {
		sort.SliceStable(keys, func(i, j int) bool {
			return len(keys[i]) > len(keys[j])
		})
	})

	// Remove only empty dirs, if parent dir is from tracked dirs
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
		if err := fs.Remove(dir); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

// deleteObject from manifest and filesystem.
func (m *Manager) deleteObject(manifest model.ObjectManifest) error {
	errors := utils.NewMultiError()

	// Remove manifest from manifest content
	m.manifest.DeleteRecord(manifest)

	// Remove dir
	if err := m.fs.Remove(manifest.Path()); err != nil {
		errors.Append(utils.PrefixError(fmt.Sprintf(`cannot delete directory "%s"`, manifest.Path()), err))
	}

	return errors.ErrorOrNil()
}

func isIgnoredDir(path string, info fs.FileInfo) bool {
	base := filesystem.Base(path)
	return info.IsDir() && strings.HasPrefix(base, ".")
}
