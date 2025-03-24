package csv

import (
	"context"
	"io"
	"sync"

	"github.com/c2h5oh/datasize"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv/fastcsv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/result"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Encoder struct {
	columns     column.Columns
	writersPool *fastcsv.WritersPool
	valuesPool  *sync.Pool
	notifier    func(ctx context.Context) *notify.Notifier
}

var columnRenderer = column.NewRenderer() //nolint:gochecknoglobals // contains Jsonnet VMs deadlock.Pool

// NewEncoder creates CSV writers pool and implements encoder.Encoder
// The order of the lines is not preserved, because we use the writers pool,
// but also because there are several source nodes with a load balancer in front of them.
// In case of encoder accepts too big csv row, it returns error.
func NewEncoder(
	concurrency int,
	rowSizeLimit datasize.ByteSize,
	mapping any,
	out io.Writer,
	notifier func(ctx context.Context) *notify.Notifier,
) (*Encoder, error) {
	tableMapping, ok := mapping.(table.Mapping)
	if !ok {
		return nil, errors.Errorf("csv encoder supports only table mapping, given %v", mapping)
	}

	return &Encoder{
		columns:     tableMapping.Columns,
		writersPool: fastcsv.NewWritersPool(out, rowSizeLimit, concurrency),
		valuesPool: &sync.Pool{
			New: func() any {
				v := make([]any, len(tableMapping.Columns))
				return &v
			},
		},
		notifier: notifier,
	}, nil
}

func (w *Encoder) WriteRecord(record recordctx.Context) (result.WriteRecordResult, error) {
	// Reduce memory allocations
	values := w.valuesPool.Get().(*[]any)
	defer w.valuesPool.Put(values)

	// Map the record to tabular data
	for i, col := range w.columns {
		value, err := columnRenderer.CSVValue(col, record)
		if err != nil {
			return result.WriteRecordResult{}, errors.PrefixErrorf(err, "cannot convert column %q to CSV value", col)
		}
		(*values)[i] = value
	}

	// Encode the values to CSV format
	n, err := w.writersPool.WriteRow(values)
	if err != nil {
		var valErr fastcsv.ValueError
		if errors.As(err, &valErr) {
			columnName := w.columns[valErr.ColumnIndex].ColumnName()
			return result.WriteRecordResult{}, errors.Errorf(`cannot convert value of the column "%s" to the string: %w`, columnName, err)
		}
		var limitErr fastcsv.LimitError
		if errors.As(err, &limitErr) {
			columnName := w.columns[limitErr.ColumnIndex].ColumnName()
			return result.WriteRecordResult{}, svcerrors.NewPayloadTooLargeError(errors.Errorf(`too big CSV row, column: "%s", row limit: %s`, columnName, limitErr.Limit.HumanReadable()))
		}

		return result.WriteRecordResult{}, err
	}

	// Get notifier after successful written record
	writeRecordResult := result.NewNotifierWriteRecordResult(n, w.notifier(record.Ctx()))
	// Buffers can be released
	// Important: values slice contains reference to the body []byte buffer, so it can be released sooner.
	record.ReleaseBuffers()

	return writeRecordResult, nil
}

func (w *Encoder) Flush() error {
	return nil
}

func (w *Encoder) Close() error {
	return nil
}
