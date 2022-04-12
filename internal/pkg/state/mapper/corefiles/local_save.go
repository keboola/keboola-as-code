package corefiles

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	HideMetaFileFieldsAnnotation = `hideMetaFileFields`
)

// MapBeforeLocalSave saves tagged object (Branch, Config,ConfigRow) fields to a files.
func (m *coreFilesMapper) MapBeforeLocalSave(ctx *local.SaveContext) error {
	m.createMetaFile(ctx)
	m.createConfigFile(ctx)
	m.createDescriptionFile(ctx)
	return nil
}

// createMetaFile meta.json.
func (m *coreFilesMapper) createMetaFile(ctx *local.SaveContext) {
	if metadata := utils.MapFromTaggedFields(model.MetaFileFieldsTag, ctx.Object()); metadata != nil {
		path := ctx.State().NamingGenerator().MetaFilePath(ctx.BasePath())
		jsonFile := filesystem.NewJsonFile(path, metadata)

		// Remove hidden fields, the annotation can be set by some other mapper.
		if hiddenFields, ok := ctx.AnnotationOrNil(HideMetaFileFieldsAnnotation).([]string); ok {
			for _, field := range hiddenFields {
				jsonFile.Content.Delete(field)
			}
		}

		ctx.ToSave().
			Add(jsonFile).
			AddTag(local.FileTypeJson).
			AddTag(local.FileKindObjectMeta)
	}
}

// createConfigFile config.json.
func (m *coreFilesMapper) createConfigFile(ctx *local.SaveContext) {
	if configuration := utils.MapFromOneTaggedField(model.ConfigFileFieldTag, ctx.Object()); configuration != nil {
		path := ctx.State().NamingGenerator().ConfigFilePath(ctx.BasePath())
		jsonFile := filesystem.NewJsonFile(path, configuration)
		ctx.ToSave().
			Add(jsonFile).
			AddTag(local.FileTypeJson).
			AddTag(local.FileKindObjectConfig)
	}
}

// createDescriptionFile description.md.
func (m *coreFilesMapper) createDescriptionFile(ctx *local.SaveContext) {
	if description, found := utils.StringFromOneTaggedField(model.DescriptionFileFieldTag, ctx.Object()); found {
		path := ctx.State().NamingGenerator().DescriptionFilePath(ctx.BasePath())
		markdownFile := filesystem.NewRawFile(path, strings.TrimRight(description, " \r\n\t")+"\n")
		ctx.ToSave().
			Add(markdownFile).
			AddTag(local.FileTypeMarkdown).
			AddTag(local.FileKindObjectDescription)
	}
}
