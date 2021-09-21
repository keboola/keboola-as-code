package local

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
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
	if err := m.manifest.PersistRecord(record); err != nil {
		return err
	}

	// Mkdir
	dir := filepath.Join(m.ProjectDir(), record.RelativePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		errors.Append(fmt.Errorf("cannot create directory \"%s\": %w", dir, err))
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

	// Write description file
	if description, found := utils.StringFromOneTaggedField(model.DescriptionFileTag, source); found {
		errPrefix := record.Kind().Name + " description"
		if err := m.writeFile(m.Naming().DescriptionFilePath(record.RelativePath()), description, errPrefix); err != nil {
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

func (m *Manager) writeFile(relPath string, content string, errPrefix string) error {
	if err := utils.WriteFile(m.ProjectDir(), relPath, content, errPrefix); err == nil {
		m.logger.Debugf(`Saved "%s"`, relPath)
	} else {
		return err
	}
	return nil
}

func (m *Manager) writeJsonFile(relPath string, content *orderedmap.OrderedMap, errPrefix string) error {
	if err := json.WriteFile(m.ProjectDir(), relPath, content, errPrefix); err == nil {
		m.logger.Debugf(`Saved "%s"`, relPath)
	} else {
		return err
	}
	return nil
}
