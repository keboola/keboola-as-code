package corefiles

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
)

const HideMetaFileFieldsAnnotation = `hideMetaFileFields`

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

// MapBeforeLocalSave saves tagged object (Branch, Config,ConfigRow) fields to a files.
func (m *mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	m.createMetaFile(recipe)
	m.createConfigFile(recipe)
	m.createDescriptionFile(recipe)
	return nil
}

// createMetaFile meta.json.
func (m *mapper) createMetaFile(recipe *model.LocalSaveRecipe) {
	if metadata := reflecthelper.MapFromTaggedFields(model.MetaFileFieldsTag, recipe.Object); metadata != nil {
		path := m.state.NamingGenerator().MetaFilePath(recipe.Path())
		jsonFile := filesystem.NewJsonFile(path, metadata)

		// Remove hidden fields, the annotation can be set by some other mapper.
		if hiddenFields, ok := recipe.Annotations[HideMetaFileFieldsAnnotation].([]string); ok {
			for _, field := range hiddenFields {
				jsonFile.Content.Delete(field)
			}
		}

		recipe.Files.
			Add(jsonFile).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindObjectMeta)
	}
}

// createConfigFile config.json.
func (m *mapper) createConfigFile(recipe *model.LocalSaveRecipe) {
	if configuration := reflecthelper.MapFromOneTaggedField(model.ConfigFileFieldTag, recipe.Object); configuration != nil {
		path := m.state.NamingGenerator().ConfigFilePath(recipe.Path())
		jsonFile := filesystem.NewJsonFile(path, configuration)
		recipe.Files.
			Add(jsonFile).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindObjectConfig)
	}
}

// createDescriptionFile description.md.
func (m *mapper) createDescriptionFile(recipe *model.LocalSaveRecipe) {
	if description, found := reflecthelper.StringFromOneTaggedField(model.DescriptionFileFieldTag, recipe.Object); found {
		path := m.state.NamingGenerator().DescriptionFilePath(recipe.Path())
		markdownFile := filesystem.NewRawFile(path, strings.TrimRight(description, " \r\n\t")+"\n")
		recipe.Files.
			Add(markdownFile).
			AddTag(model.FileTypeMarkdown).
			AddTag(model.FileKindObjectDescription)
	}
}
