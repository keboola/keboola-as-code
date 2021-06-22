package local

import (
	"go.uber.org/zap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
)

func SaveModel(logger *zap.SugaredLogger, m *manifest.Manifest, record manifest.Record, source interface{}) *utils.Error {
	errors := &utils.Error{}

	// Add record to manifest
	m.AddRecord(record)
	paths := record.GetPaths()

	// Mkdir
	if err := os.MkdirAll(paths.RelativePath(), 0755); err != nil {
		errors.Add(err)
		return errors
	}

	// Write metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, source); metadata != nil {
		if err := json.WriteFile(m.ProjectDir, record.MetaFilePath(), metadata, record.Kind()+" metadata"); err == nil {
			logger.Debugf(`Saved "%s"`, record.MetaFilePath())
		} else {
			errors.Add(err)
		}
	}

	// Write config file
	if config := utils.MapFromOneTaggedField(model.ConfigFileTag, source); config != nil {
		if err := json.WriteFile(m.ProjectDir, record.ConfigFilePath(), config, record.Kind()); err == nil {
			logger.Debugf(`Saved "%s"`, record.ConfigFilePath())
		} else {
			errors.Add(err)
		}
	}

	if errors.Len() > 0 {
		return errors
	}

	return nil
}
