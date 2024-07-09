package csv

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/format"
	fastcsv2 "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/format/csv/fastcsv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Writer struct {
	columns column.Columns
	pool    *fastcsv2.WritersPool
}

// NewWriter creates CSV writers pool and implements format.Writer
// The order of the lines is not preserved, because we use the writers pool,
// but also because there are several source nodes with a load balancer in front of them.
func NewWriter(cfg format.Config, out io.Writer, slice *model.Slice) (format.Writer, error) {
	return &Writer{
		columns: slice.Columns,
		pool:    fastcsv2.NewWritersPool(out, cfg.Concurrency),
	}, nil
}

func (w *Writer) WriteRecord(values []any) error {
	err := w.pool.WriteRow(&values)
	if err != nil {
		var valErr fastcsv2.ValueError
		if errors.As(err, &valErr) {
			columnName := w.columns[valErr.ColumnIndex].ColumnName()
			return errors.Errorf(`cannot convert value of the column "%s" to the string: %w`, columnName, err)
		}
		return err
	}

	return nil
}

func (w *Writer) Flush() error {
	return nil
}

func (w *Writer) Close() error {
	return nil
}
