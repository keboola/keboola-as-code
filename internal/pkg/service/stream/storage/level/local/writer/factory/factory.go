// Package factory create a slice writer according to the specified file type.
package factory

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Factory func(w *writer.BaseWriter) (writer.Writer, error)

func Default(w *writer.BaseWriter) (writer.Writer, error) {
	// Create writer according to the file type
	switch w.Type() {
	case model.FileTypeCSV:
		return csv.NewWriter(w)
	default:
		return nil, errors.Errorf(`unexpected file type "%s"`, w.Type())
	}
}
