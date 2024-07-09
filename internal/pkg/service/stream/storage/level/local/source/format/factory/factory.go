package factory

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/format"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/format/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Default implements format.WriterFactory.
func Default(cfg format.Config, out io.Writer, slice *model.Slice) (format.Writer, error) {
	switch slice.Type {
	case model.FileTypeCSV:
		return csv.NewWriter(cfg, out, slice)
	default:
		return nil, errors.Errorf(`unexpected file type "%s"`, slice.Type)
	}
}
