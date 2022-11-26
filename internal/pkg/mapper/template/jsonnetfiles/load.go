package jsonnetfiles

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ctxKey string

const FileDefCtxKey = ctxKey("fileDef")

func (m *jsonNetMapper) LoadLocalFile(def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error) {
	// Load JsonNet file instead of Json file
	if def.HasTag(model.FileTypeJSON) {
		// Modify metadata
		def.RemoveTag(model.FileTypeJSON)
		def.AddTag(model.FileTypeJSONNET)
		def.SetPath(strings.TrimSuffix(def.Path(), `.json`) + `.jsonnet`)

		// Load JsonNet file
		f, err := next(def, filesystem.FileTypeJSONNET)
		if err != nil {
			return nil, err
		}
		jsonNetFile := f.(*filesystem.JSONNETFile)

		// Set context (ctx, variables, ...)
		ctx := m.jsonNetCtx.Ctx()
		ctx = context.WithValue(ctx, FileDefCtxKey, def)
		jsonNetFile.SetContext(m.jsonNetCtx.WithCtx(ctx))

		// Convert to Json/Raw
		switch fileType {
		case filesystem.FileTypeRaw:
			return jsonNetFile.ToRawFile()
		case filesystem.FileTypeJSON:
			return jsonNetFile.ToJSONFile()
		default:
			panic(errors.Errorf(`unexpected filesystem.FileType = %v`, fileType))
		}
	}

	return next(def, fileType)
}
