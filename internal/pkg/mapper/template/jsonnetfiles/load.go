package jsonnetfiles

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *jsonNetMapper) LoadLocalFile(def *filesystem.FileDef, fileType filesystem.FileType, next fileloader.Handler) (filesystem.File, error) {
	// Load JsonNet file instead of Json file
	if def.HasTag(model.FileTypeJson) {
		// Modify metadata
		def.RemoveTag(model.FileTypeJson)
		def.AddTag(model.FileTypeJsonNet)
		def.SetPath(strings.TrimSuffix(def.Path(), `.json`) + `.jsonnet`)

		// Load JsonNet file
		f, err := next(def, filesystem.FileTypeJsonNet)
		if err != nil {
			return nil, err
		}
		jsonNetFile := f.(*filesystem.JsonNetFile)

		// Convert to Json/Raw
		switch fileType {
		case filesystem.FileTypeRaw:
			return jsonNetFile.ToRawFile()
		case filesystem.FileTypeJson:
			return jsonNetFile.ToJsonFile()
		default:
			panic(fmt.Errorf(`unexpected filesystem.FileType = %v`, fileType))
		}
	}

	return next(def, fileType)
}
