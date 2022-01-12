package corefiles

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapBeforeLocalSave saves tagged object (Branch, Config,ConfigRow) fields to a files.
func (m *coreFilesMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	m.createMetaFile(recipe)
	m.createConfigFile(recipe)
	m.createDescriptionFile(recipe)
	return nil
}

// createMetaFile meta.json.
func (m *coreFilesMapper) createMetaFile(recipe *model.LocalSaveRecipe) {
	if metadata := utils.MapFromTaggedFields(model.MetaFileFieldsTag, recipe.Object); metadata != nil {
		path := m.state.NamingGenerator().MetaFilePath(recipe.Path())
		recipe.Files.
			Add(filesystem.NewJsonFile(path, metadata)).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindObjectMeta)
	}
}

// createConfigFile config.json.
func (m *coreFilesMapper) createConfigFile(recipe *model.LocalSaveRecipe) {
	if configuration := utils.MapFromOneTaggedField(model.ConfigFileFieldTag, recipe.Object); configuration != nil {
		path := m.state.NamingGenerator().ConfigFilePath(recipe.Path())
		recipe.Files.
			Add(filesystem.NewJsonFile(path, configuration)).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindObjectConfig)
	}
}

// createDescriptionFile description.md.
func (m *coreFilesMapper) createDescriptionFile(recipe *model.LocalSaveRecipe) {
	if description, found := utils.StringFromOneTaggedField(model.DescriptionFileFieldTag, recipe.Object); found {
		path := m.state.NamingGenerator().DescriptionFilePath(recipe.Path())
		recipe.Files.
			Add(filesystem.NewFile(path, strings.TrimRight(description, " \r\n\t")+"\n")).
			AddTag(model.FileTypeMarkdown).
			AddTag(model.FileKindObjectDescription)
	}
}
