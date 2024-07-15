package encoder

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"

// Encoder writers record values as bytes to the underlying writer.
// It is used inside the Writer pipeline, at the beginning, before the compression.
type Encoder interface {
	WriteRecord(record recordctx.Context) error
	Flush() error
	Close() error
}
