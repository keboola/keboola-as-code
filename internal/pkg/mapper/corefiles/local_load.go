package corefiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapAfterLocalLoad loads files to tagged object (Branch, Config,ConfigRow) fields.
func (m *coreFilesMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	errors := utils.NewMultiError()
	if err := m.loadMetaFile(recipe); err != nil {
		errors.Append(err)
	}
	if err := m.loadConfigFile(recipe); err != nil {
		errors.Append(err)
	}
	if err := m.loadDescriptionFile(recipe); err != nil {
		errors.Append(err)
	}
	return errors.ErrorOrNil()
}

// loadMetaFile from meta.json.
func (m *coreFilesMapper) loadMetaFile(recipe *model.LocalLoadRecipe) error {
	path := m.state.NamingGenerator().MetaFilePath(recipe.ObjectManifest.Path())
	desc := recipe.ObjectManifest.Kind().Name + " metadata"
	if file, err := m.state.Fs().ReadJsonFieldsTo(path, desc, recipe.Object, model.MetaFileFieldsTag); err != nil {
		return err
	} else if file != nil {
		recipe.Files.
			Add(file).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindObjectMeta)
	}
	return nil
}

// loadConfigFile from config.json.
func (m *coreFilesMapper) loadConfigFile(recipe *model.LocalLoadRecipe) error {
	// config.json
	path := m.state.NamingGenerator().ConfigFilePath(recipe.ObjectManifest.Path())
	desc := recipe.ObjectManifest.Kind().Name
	if file, err := m.state.Fs().ReadJsonMapTo(path, desc, recipe.Object, model.ConfigFileFieldTag); err != nil {
		return err
	} else if file != nil {
		recipe.Files.
			Add(file).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindObjectConfig)
	}
	return nil
}

// loadDescriptionFile from description.md.
func (m *coreFilesMapper) loadDescriptionFile(recipe *model.LocalLoadRecipe) error {
	path := m.state.NamingGenerator().DescriptionFilePath(recipe.ObjectManifest.Path())
	desc := recipe.ObjectManifest.Kind().Name + " description"
	if file, err := m.state.Fs().ReadFileContentTo(path, desc, recipe.Object, model.DescriptionFileFieldTag); err != nil {
		return err
	} else if file != nil {
		recipe.Files.
			Add(file).
			AddTag(model.FileTypeMarkdown).
			AddTag(model.FileKindObjectDescription)
	}
	return nil
}
