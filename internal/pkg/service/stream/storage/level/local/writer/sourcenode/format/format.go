package format

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// WriterFactory creates Writer according to the configuration and slice entity.
type WriterFactory func(cfg Config, out io.Writer, slice *model.Slice) (Writer, error)

// Writer writers record values as bytes to the underlying writer.
// It is used inside the Writer.
type Writer interface {
	WriteRecord(values []any) error
	Flush() error
	Close() error
}

func NewConfig() Config {
	return Config{
		Concurrency: 0, // 0 = auto = CPU cores
	}
}
