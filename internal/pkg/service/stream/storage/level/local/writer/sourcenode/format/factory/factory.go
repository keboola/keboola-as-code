package factory

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/format/csv"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Default implements format.FormatWriterFactory.
func Default(cfg writer.Config, out io.Writer, slice *model.Slice) (writer.FormatWriter, error) {
	switch slice.Type {
	case model.FileTypeCSV:
		return csv.NewWriter(cfg, out, slice)
	default:
		return nil, errors.Errorf(`unexpected file type "%s"`, slice.Type)
	}
}
