package local

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DeleteModel from manifest and disk
func DeleteModel(logger *zap.SugaredLogger, m *manifest.Manifest, record manifest.Record, target interface{}) error {
	errors := &utils.Error{}

	// Remove from manifest content
	m.DeleteRecord(record)

	// Delete metadata file
	metaFields := utils.GetFieldsWithTag(model.MetaFileTag, target)
	if len(metaFields) > 0 {
		if err := os.Remove(filepath.Join(m.ProjectDir, record.MetaFilePath())); err == nil {
			logger.Debugf("Removed \"%s\"", record.MetaFilePath())
		} else if !os.IsNotExist(err) {
			errors.Add(err)
		}
	}

	// Delete config file
	configField := utils.GetOneFieldWithTag(model.ConfigFileTag, target)
	if configField != nil {
		if err := os.Remove(filepath.Join(m.ProjectDir, record.ConfigFilePath())); err == nil {
			logger.Debugf("Removed \"%s\"", record.ConfigFilePath())
		} else if !os.IsNotExist(err) {
			errors.Add(err)
		}
	}

	// Delete dir
	dir := filepath.Join(m.ProjectDir, record.GetPaths().RelativePath())
	if err := os.RemoveAll(dir); err != nil {
		errors.Add(fmt.Errorf("cannot remove directory \"%s\": %s", dir, err))
	}

	if errors.Len() > 0 {
		return errors
	}

	return nil
}

// DeleteEmptyDirectories from project directory (eg. dir with extractors, but no extractor left)
func DeleteEmptyDirectories(logger *zap.SugaredLogger, projectDir string) error {
	errors := &utils.Error{}
	dirs := utils.NewOrderedMap()
	err := filepath.WalkDir(projectDir, func(path string, d os.DirEntry, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == projectDir {
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
			logger.Debugf(`Deleted "%s"`, utils.RelPath(projectDir, dir))
		} else {
			errors.Add(err)
		}
	}

	if errors.Len() > 0 {
		return errors
	}

	return nil
}
