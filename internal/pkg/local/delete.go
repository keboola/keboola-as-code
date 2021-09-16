package local

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// DeleteModel from manifest and disk.
func (m *Manager) DeleteModel(record model.Record) error {
	errors := utils.NewMultiError()

	// Remove record from manifest content
	m.manifest.DeleteRecord(record)

	// Metadata file
	if err := m.remove(m.Naming().MetaFilePath(record.RelativePath())); err != nil {
		errors.Append(err)
	}

	// Config file
	if err := m.remove(m.Naming().ConfigFilePath(record.RelativePath())); err != nil {
		errors.Append(err)
	}

	// Dir
	if err := m.removeAll(record.RelativePath()); err != nil {
		errors.Append(utils.PrefixError(fmt.Sprintf(`cannot delete directory "%s"`, record.RelativePath()), err))
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
			if err := m.DeleteModel(record); err != nil {
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
	err := filepath.WalkDir(m.ProjectDir(), func(path string, d os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == m.ProjectDir() {
			return nil
		}

		// Stop on ignored dir
		isIgnoredDir := utils.IsIgnoredDir(path, d)

		// Found a directory -> store path
		if !isIgnoredDir && d.IsDir() {
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
		if isIgnoredDir {
			return filepath.SkipDir
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
			m.logger.Debugf(`Removed "%s"`, utils.RelPath(m.ProjectDir(), dir))
		} else {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func (m *Manager) remove(relPath string) error {
	absPath := filepath.Join(m.ProjectDir(), relPath)
	if err := os.Remove(absPath); err == nil {
		m.logger.Debugf("Removed \"%s\"", relPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *Manager) removeAll(relPath string) error {
	absPath := filepath.Join(m.ProjectDir(), relPath)
	if err := os.RemoveAll(absPath); err == nil {
		m.logger.Debugf("Removed \"%s\"", relPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}
