package corefiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
)

// MapAfterLocalLoad loads files to tagged object (Branch, Config,ConfigRow) fields.
func (m *coreFilesMapper) MapAfterLocalLoad(ctx *local.LoadContext) error {
	errs := errors.NewMultiError()
	if err := m.loadMetaFile(ctx); err != nil {
		errs.Append(err)
	}
	if err := m.loadConfigFile(ctx); err != nil {
		errs.Append(err)
	}
	if err := m.loadDescriptionFile(ctx); err != nil {
		errs.Append(err)
	}
	return errs.ErrorOrNil()
}

// loadMetaFile from meta.json.
func (m *coreFilesMapper) loadMetaFile(ctx *local.LoadContext) error {
	_, _, err := ctx.
		Load(ctx.State().NamingGenerator().MetaFilePath(ctx.BasePath())).
		SetDescription(ctx.Object().Kind().Name+" metadata").
		AddTag(local.FileTypeJson).
		AddTag(local.FileKindObjectMeta).
		ReadJsonFieldsTo(ctx.Object(), model.MetaFileFieldsTag)
	return err
}

// loadConfigFile from config.json.
func (m *coreFilesMapper) loadConfigFile(ctx *local.LoadContext) error {
	_, _, err := ctx.
		Load(ctx.State().NamingGenerator().ConfigFilePath(ctx.BasePath())).
		SetDescription(ctx.Object().Kind().Name).
		AddTag(local.FileTypeJson).
		AddTag(local.FileKindObjectConfig).
		ReadJsonMapTo(ctx.Object(), model.ConfigFileFieldTag)
	return err
}

// loadDescriptionFile from description.md.
func (m *coreFilesMapper) loadDescriptionFile(ctx *local.LoadContext) error {
	_, _, err := ctx.
		Load(ctx.State().NamingGenerator().DescriptionFilePath(ctx.BasePath())).
		SetDescription(ctx.Object().Kind().Name+" description").
		AddTag(local.FileTypeMarkdown).
		AddTag(local.FileKindObjectDescription).
		ReadFileContentTo(ctx.Object(), model.DescriptionFileFieldTag)
	return err
}
