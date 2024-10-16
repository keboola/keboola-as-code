package encoder

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/result"
)

// Encoder writers record values as bytes to the underlying writer.
// It is used inside the Writer pipeline, at the beginning, before the compression.
type Encoder interface {
	WriteRecord(record recordctx.Context) (result.WriteRecordResult, error)
	Flush() error
	Close() error
}
