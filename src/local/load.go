package local

import (
	"keboola-as-code/src/json"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"reflect"
)

func LoadModel(projectDir string, record manifest.Record, target interface{}) *utils.Error {
	errors := &utils.Error{}

	// Load values from meta file
	errPrefix := record.Kind() + " metadata"
	if err := utils.ReadTaggedFields(projectDir, record.MetaFilePath(), model.MetaFileTag, errPrefix, target); err != nil {
		errors.Add(err)
	}

	// Load config file content
	errPrefix = record.Kind()
	if configField := utils.GetOneFieldWithTag(model.ConfigFileTag, target); configField != nil {
		content := utils.EmptyOrderedMap()
		modelValue := reflect.ValueOf(target).Elem()
		if err := json.ReadFile(projectDir, record.ConfigFilePath(), &content, errPrefix); err == nil {
			modelValue.FieldByName(configField.Name).Set(reflect.ValueOf(content))
		} else {
			errors.Add(err)
		}
	}

	if errors.Len() > 0 {
		return errors
	}

	return nil
}

func LoadBranch(projectDir string, b *manifest.BranchManifest) (*model.Branch, *utils.Error) {
	branch := &model.Branch{Id: b.Id}
	if err := LoadModel(projectDir, b, branch); err != nil {
		return nil, err
	}
	return branch, nil
}

func LoadConfig(projectDir string, c *manifest.ConfigManifest) (*model.Config, *utils.Error) {
	config := &model.Config{BranchId: c.BranchId, ComponentId: c.ComponentId, Id: c.Id}
	if err := LoadModel(projectDir, c, config); err != nil {
		return nil, err
	}
	return config, nil
}

func LoadConfigRow(projectDir string, r *manifest.ConfigRowManifest) (*model.ConfigRow, *utils.Error) {
	row := &model.ConfigRow{BranchId: r.BranchId, ComponentId: r.ComponentId, ConfigId: r.ConfigId, Id: r.Id}
	if err := LoadModel(projectDir, r, row); err != nil {
		return nil, err
	}
	return row, nil
}
