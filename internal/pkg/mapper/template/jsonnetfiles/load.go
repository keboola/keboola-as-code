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

func (m *jsonnetMapper) LoadLocalFile(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error) {
	// Load Jsonnet file instead of Json file
	if def.HasTag(model.FileTypeJSON) {
		// Modify metadata
		def.RemoveTag(model.FileTypeJSON)
		def.AddTag(model.FileTypeJsonnet)
		def.SetPath(strings.TrimSuffix(def.Path(), `.json`) + `.jsonnet`)

		// Load Jsonnet file
		f, err := next(ctx, def, filesystem.FileTypeJsonnet)
		if err != nil {
			return nil, err
		}
		jsonnetFile := f.(*filesystem.JsonnetFile)

		// Set context (ctx, variables, ...)
		ctx = context.WithValue(ctx, FileDefCtxKey, def)
		jsonnetFile.SetContext(m.jsonnetCtx.WithCtx(ctx))

		// Convert to Json/Raw
		switch fileType {
		case filesystem.FileTypeRaw:
			return jsonnetFile.ToRawFile()
		case filesystem.FileTypeJSON:
			return jsonnetFile.ToJSONFile()
		default:
			panic(errors.Errorf(`unexpected filesystem.FileType = %v`, fileType))
		}
	}

	return next(ctx, def, fileType)
}
