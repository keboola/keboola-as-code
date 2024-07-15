package csv

import (
	"io"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv/fastcsv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Encoder struct {
	columns     column.Columns
	writersPool *fastcsv.WritersPool
	valuesPool  *sync.Pool
}

var columnRenderer = column.NewRenderer() //nolint:gochecknoglobals // contains Jsonnet VMs sync.Pool

// NewEncoder creates CSV writers pool and implements encoder.Encoder
// The order of the lines is not preserved, because we use the writers pool,
// but also because there are several source nodes with a load balancer in front of them.
func NewEncoder(concurrency int, out io.Writer, slice *model.Slice) (*Encoder, error) {
	return &Encoder{
		columns:     slice.Columns,
		writersPool: fastcsv.NewWritersPool(out, concurrency),
		valuesPool: &sync.Pool{
			New: func() any {
				v := make([]any, len(slice.Columns))
				return &v
			},
		},
	}, nil
}

func (w *Encoder) WriteRecord(record recordctx.Context) error {
	// Reduce memory allocations
	values := w.valuesPool.Get().(*[]any)
	defer w.valuesPool.Put(values)

	// Map the record to tabular data
	for i, col := range w.columns {
		str, err := columnRenderer.CSVValue(col, record)
		if err != nil {
			return errors.PrefixErrorf(err, "cannot convert column %q to CSV value", col)
		}
		(*values)[i] = str
	}

	// Encode the values to CSV format
	err := w.writersPool.WriteRow(values)
	if err != nil {
		var valErr fastcsv.ValueError
		if errors.As(err, &valErr) {
			columnName := w.columns[valErr.ColumnIndex].ColumnName()
			return errors.Errorf(`cannot convert value of the column "%s" to the string: %w`, columnName, err)
		}
		return err
	}

	return nil
}

func (w *Encoder) Flush() error {
	return nil
}

func (w *Encoder) Close() error {
	return nil
}
