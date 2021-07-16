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
func (m *Manager) DeleteEmptyDirectories() error {
	errors := utils.NewMultiError()
	dirs := utils.NewOrderedMap()
	err := filepath.WalkDir(m.ProjectDir(), func(path string, d os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == m.ProjectDir() {
			return nil
		}

		// Found a directory -> store path
		if d.IsDir() {
			dirs.Set(path+string(filepath.Separator), true)
			return nil
		}

		// If file, remove from "dirs" all file parents
		for _, dir := range dirs.Keys() {
			if strings.HasPrefix(path, dir) {
				dirs.Delete(dir)
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Sort longest path firs -> most nested directory first
	dirs.SortKeys(func(keys []string) {
		sort.SliceStable(keys, func(i, j int) bool {
			return len(keys[i]) > len(keys[j])
		})
	})

	// Only empty directories remain in "dirs" -> delete
	for _, dir := range dirs.Keys() {
		if err := os.Remove(dir); err == nil {
			m.logger.Debugf(`Deleted "%s"`, utils.RelPath(m.ProjectDir(), dir))
		} else {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}
