package local

import (
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
)

// SaveModel to manifest and disk
func SaveModel(logger *zap.SugaredLogger, m *manifest.Manifest, record manifest.Record, source model.ValueWithKey) error {
	errors := &utils.Error{}
	paths := record.GetPaths()

	// Add record to manifest content
	m.SetRecord(record)

	// Mkdir
	dir := filepath.Join(m.ProjectDir, paths.RelativePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		errors.Add(fmt.Errorf("cannot create directory \"%s\": %s", dir, err))
		return errors
	}

	// Write metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, source); metadata != nil {
		if err := json.WriteFile(m.ProjectDir, record.MetaFilePath(), metadata, record.Kind().Name+" metadata"); err == nil {
			logger.Debugf(`Saved "%s"`, record.MetaFilePath())
		} else {
			errors.Add(err)
		}
	}

	// Write config file
	if config := utils.MapFromOneTaggedField(model.ConfigFileTag, source); config != nil {
		if err := json.WriteFile(m.ProjectDir, record.ConfigFilePath(), config, record.Kind().Name); err == nil {
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
