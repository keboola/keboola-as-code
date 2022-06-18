package corefiles

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapAfterLocalLoad loads files to tagged object (Branch, Config,ConfigRow) fields.
func (m *coreFilesMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
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
	_, _, err := recipe.Files.
		Load(m.state.NamingGenerator().MetaFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name+" metadata").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindObjectMeta).
		ReadJsonFieldsTo(recipe.Object, model.MetaFileFieldsTag)
	return err
}

// loadConfigFile from config.json.
func (m *coreFilesMapper) loadConfigFile(recipe *model.LocalLoadRecipe) error {
	_, _, err := recipe.Files.
		Load(m.state.NamingGenerator().ConfigFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name).
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindObjectConfig).
		ReadJsonMapTo(recipe.Object, model.ConfigFileFieldTag)
	return err
}

// loadDescriptionFile from description.md.
func (m *coreFilesMapper) loadDescriptionFile(recipe *model.LocalLoadRecipe) error {
	_, _, err := recipe.Files.
		Load(m.state.NamingGenerator().DescriptionFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name+" description").
		AddTag(model.FileTypeMarkdown).
		AddTag(model.FileKindObjectDescription).
		ReadFileContentTo(recipe.Object, model.DescriptionFileFieldTag)
	return err
}
