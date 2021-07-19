package local

import (
	"fmt"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DeleteModel from manifest and disk
func (m *Manager) DeleteModel(record manifest.Record) error {
	errors := utils.NewMultiError()

	// Remove record from manifest content
	m.manifest.DeleteRecord(record)

	// Delete metadata file
	if err := os.Remove(filepath.Join(m.ProjectDir(), record.MetaFilePath())); err == nil {
		m.logger.Debugf("Removed \"%s\"", record.MetaFilePath())
	} else if !os.IsNotExist(err) {
		errors.Append(err)
	}

	// Delete config file
	if err := os.Remove(filepath.Join(m.ProjectDir(), record.ConfigFilePath())); err == nil {
		m.logger.Debugf("Removed \"%s\"", record.ConfigFilePath())
	} else if !os.IsNotExist(err) {
		errors.Append(err)
	}

	// Delete dir
	dir := filepath.Join(m.ProjectDir(), record.RelativePath())
	if err := os.RemoveAll(dir); err != nil {
		errors.Append(fmt.Errorf("cannot remove directory \"%s\": %s", dir, err))
	}

	return errors.ErrorOrNil()
}

// DeleteEmptyDirectories from project directory (eg. dir with extractors, but no extractor left)
// Deleted are only empty directories from know/tracked paths.
// Hidden dirs are ignored.
func (m *Manager) DeleteEmptyDirectories(trackedPaths []string) error {
	errors := utils.NewMultiError()
	emptyDirs := utils.NewOrderedMap()
	err := filepath.WalkDir(m.ProjectDir(), func(path string, d os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == m.ProjectDir() {
			return nil
		}

		// Skip ignored
		if utils.IsIgnoredDir(path, d) {
			return filepath.SkipDir
		}

		// Found a directory -> store path
		if d.IsDir() {
			emptyDirs.Set(path+string(os.PathSeparator), true)
			return nil
		}

		// Is file -> all parent dirs are not empty
		for _, dir := range emptyDirs.Keys() {
			if strings.HasPrefix(path, dir) {
				emptyDirs.Delete(dir)
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Sort longest path firs -> delete most nested directory first
	emptyDirs.SortKeys(func(keys []string) {
		sort.SliceStable(keys, func(i, j int) bool {
			return len(keys[i]) > len(keys[j])
		})
	})

	// Remove only empty dirs, if parent dir is from tracked dirs
	dirsToRemove := make([]string, 0)
	for _, dir := range emptyDirs.Keys() {
		for _, tracked := range trackedPaths {
			prefix := filepath.Join(m.ProjectDir(), tracked) + string(os.PathSeparator)
			if strings.HasPrefix(dir, prefix) {
				// Remove dir, it is from a tracked dir
				dirsToRemove = append(dirsToRemove, dir)
				break
			}
		}
	}

	// Delete
	for _, dir := range dirsToRemove {
		if err := os.Remove(dir); err == nil {
			m.logger.Debugf(`Deleted "%s"`, utils.RelPath(m.ProjectDir(), dir))
		} else {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}
