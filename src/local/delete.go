package local

import (
	"go.uber.org/zap"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
)

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

	if errors.Len() > 0 {
		return errors
	}

	return nil
}
