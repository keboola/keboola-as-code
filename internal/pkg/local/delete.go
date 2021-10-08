package local

import (
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// DeleteObject from manifest and filesystem.
func (m *Manager) DeleteObject(record model.Record) error {
	errors := utils.NewMultiError()

	// Remove record from manifest content
	m.manifest.DeleteRecord(record)

	// Remove dir
	if err := m.fs.Remove(record.Path()); err != nil {
		errors.Append(utils.PrefixError(fmt.Sprintf(`cannot delete directory "%s"`, record.Path()), err))
	}

	return errors.ErrorOrNil()
}

// DeleteInvalidObjects from disk, eg. if pull --force used.
func (m *Manager) DeleteInvalidObjects() error {
	errors := utils.NewMultiError()
	records := m.manifest.GetRecords()
	for _, key := range append([]string(nil), records.Keys()...) {
		v, _ := records.Get(key)
		record := v.(model.Record)
		if record.State().IsInvalid() {
			if err := m.DeleteObject(record); err != nil {
				errors.Append(err)
			}
		}
	}
	return errors.ErrorOrNil()
}

// DeleteEmptyDirectories from project directory (eg. dir with extractors, but no extractor left)
// Deleted are only empty directories from know/tracked paths.
// Hidden dirs are ignored.
func (m *Manager) DeleteEmptyDirectories(trackedPaths []string) error {
	errors := utils.NewMultiError()
	emptyDirs := utils.NewOrderedMap()
	root := `.`
	err := m.fs.Walk(root, func(path string, info fs.FileInfo, err error) error {
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
			emptyDirs.Set(path+string(os.PathSeparator), true)
			return nil
		}

		// Found file/ignored dir -> all parent dirs are not empty
		for _, dir := range emptyDirs.Keys() {
			if strings.HasPrefix(path, dir) {
				emptyDirs.Delete(dir)
			}
		}

		// Skip sub-directories
		if skipDir {
			return fs.SkipDir
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
			prefix := tracked + string(os.PathSeparator)
			if strings.HasPrefix(dir, prefix) {
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
