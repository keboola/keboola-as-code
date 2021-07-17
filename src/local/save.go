package local

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/transformation"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
)

// SaveModel to manifest and disk
func (m *Manager) SaveModel(record manifest.Record, source model.ValueWithKey) error {
	errors := utils.NewMultiError()

	// Validate
	if err := validator.Validate(source); err != nil {
		errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, record.Kind().Name, record.RelativePath()), err)
		return errors
	}

	// Add record to manifest content + mark it for saving
	m.manifest.PersistRecord(record)

	// Mkdir
	dir := filepath.Join(m.ProjectDir(), record.RelativePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		errors.Append(fmt.Errorf("cannot create directory \"%s\": %s", dir, err))
		return errors
	}

	// Transform
	configContent, err := m.transformOnSave(record, source)
	if err != nil {
		errors.Append(err)
	}

	// Write metadata file
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, source); metadata != nil {
		if err := json.WriteFile(m.ProjectDir(), record.MetaFilePath(), metadata, record.Kind().Name+" metadata"); err == nil {
			m.logger.Debugf(`Saved "%s"`, record.MetaFilePath())
		} else {
			errors.Append(err)
		}
	}

	// Write config file (from transformOnSave if defined)
	if configContent == nil {
		configContent = utils.MapFromOneTaggedField(model.ConfigFileTag, source)
	}
	if configContent != nil {
		if err := json.WriteFile(m.ProjectDir(), record.ConfigFilePath(), configContent, record.Kind().Name); err == nil {
			m.logger.Debugf(`Saved "%s"`, record.ConfigFilePath())
		} else {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func (m *Manager) transformOnSave(record manifest.Record, source model.ValueWithKey) (*orderedmap.OrderedMap, error) {
	if ok, err := m.isTransformationConfig(source); ok {
		return transformation.SaveBlocks(
			m.ProjectDir(),
			m.logger,
			m.Naming(),
			record.(*manifest.ConfigManifest),
			source.(*model.Config),
		)
	} else if err != nil {
		return nil, err
	}
	return nil, nil
}
