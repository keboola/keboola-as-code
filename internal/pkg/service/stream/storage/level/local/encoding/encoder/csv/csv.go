package csv

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv/fastcsv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Encoder struct {
	columns column.Columns
	pool    *fastcsv.WritersPool
}

// NewEncoder creates CSV writers pool and implements encoder.Encoder
// The order of the lines is not preserved, because we use the writers pool,
// but also because there are several source nodes with a load balancer in front of them.
func NewEncoder(concurrency int, out io.Writer, slice *model.Slice) (*Encoder, error) {
	return &Encoder{
		columns: slice.Columns,
		pool:    fastcsv.NewWritersPool(out, concurrency),
	}, nil
}

func (w *Encoder) WriteRecord(values []any) error {
	err := w.pool.WriteRow(&values)
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
