package writer

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// FormatWriterFactory creates FormatWriter according to the configuration and slice entity.
type FormatWriterFactory func(cfg Config, out io.Writer, slice *model.Slice) (FormatWriter, error)

// FormatWriter writers record values as bytes to the underlying writer.
// It is used inside the FormatWriter.
type FormatWriter interface {
	WriteRecord(values []any) error
	Flush() error
	Close() error
}
