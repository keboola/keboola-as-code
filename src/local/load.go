package local

import (
	"fmt"
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/transformation"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"path/filepath"
	"reflect"
)

// LoadModel from manifest and disk
func (m *Manager) LoadModel(record manifest.Record, target interface{}) (found bool, err error) {
	errors := utils.NewMultiError()

	// Check if directory exists
	if !utils.IsDir(filepath.Join(m.ProjectDir(), record.RelativePath())) {
		errors.Append(fmt.Errorf(`%s "%s" not found`, record.Kind().Name, record.RelativePath()))
		return false, errors
	}

	// Load values from the meta file
	errPrefix := record.Kind().Name + " metadata"
	if err := utils.ReadTaggedFields(m.ProjectDir(), record.MetaFilePath(), model.MetaFileTag, errPrefix, target); err != nil {
		errors.Append(err)
	}

	// Load config file content
	errPrefix = record.Kind().Name
	if configField := utils.GetOneFieldWithTag(model.ConfigFileTag, target); configField != nil {
		content := utils.NewOrderedMap()
		modelValue := reflect.ValueOf(target).Elem()
		if err := json.ReadFile(m.ProjectDir(), record.ConfigFilePath(), &content, errPrefix); err == nil {
			modelValue.FieldByName(configField.Name).Set(reflect.ValueOf(content))
		} else {
			errors.Append(err)
		}
	}

	// Transform
	if err := m.transformOnLoad(record, target); err != nil {
		errors.Append(err)
	}

	// Validate, if all files loaded without error
	if errors.Len() == 0 {
		if err := validator.Validate(target); err != nil {
			errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, record.Kind().Name, record.RelativePath()), err)
		}
	}

	return true, errors.ErrorOrNil()
}

func (m *Manager) transformOnLoad(record manifest.Record, target interface{}) error {
	if ok, err := m.isTransformationConfig(target); ok {
		return transformation.LoadBlocks(
			m.ProjectDir(),
			m.Naming(),
			record.(*manifest.ConfigManifest),
			target.(*model.Config),
		)
	} else if err != nil {
		return err
	}
	return nil
}
