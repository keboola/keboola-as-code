package local

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iancoleman/orderedmap"

	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/transformation"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
)

// SaveModel to manifest and disk.
func (m *Manager) SaveModel(record model.Record, source model.Object) error {
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
		errPrefix := record.Kind().Name + " metadata"
		if err := m.writeJsonFile(m.Naming().MetaFilePath(record.RelativePath()), metadata, errPrefix); err != nil {
			errors.Append(err)
		}
	}

	// Write config file (can be modified by transformOnSave method)
	if configContent == nil {
		configContent = utils.MapFromOneTaggedField(model.ConfigFileTag, source)
	}
	if configContent != nil {
		errPrefix := record.Kind().Name
		if err := m.writeJsonFile(m.Naming().ConfigFilePath(record.RelativePath()), configContent, errPrefix); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func (m *Manager) transformOnSave(record model.Record, source model.Object) (*orderedmap.OrderedMap, error) {
	if ok, err := m.isTransformationConfig(source); ok {
		return transformation.SaveBlocks(
			m.ProjectDir(),
			m.logger,
			m.Naming(),
			record.(*model.ConfigManifest),
			source.(*model.Config),
		)
	} else if err != nil {
		return nil, err
	}
	return nil, nil
}

func (m *Manager) writeJsonFile(relPath string, content *orderedmap.OrderedMap, errPrefix string) error {
	if err := json.WriteFile(m.ProjectDir(), relPath, content, errPrefix); err == nil {
		m.logger.Debugf(`Saved "%s"`, relPath)
	} else {
		return err
	}
	return nil
}
