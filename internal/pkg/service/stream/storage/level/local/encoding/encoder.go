package encoding

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// EncoderFactory creates Writer according to the configuration and slice entity.
type EncoderFactory func(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error)

// Encoder writers record values as bytes to the underlying writer.
// It is used inside the Writer pipeline, at the beginning, before the compression.
type Encoder interface {
	WriteRecord(values []any) error
	Flush() error
	Close() error
}
