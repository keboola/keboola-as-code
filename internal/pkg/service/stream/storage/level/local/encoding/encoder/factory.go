package encoder

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Factory creates Encoder according to the configuration and slice entity.
type Factory func(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error)

// DefaultFactory implements Factory.
func DefaultFactory(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error) {
	switch slice.Type {
	case model.FileTypeCSV:
		return csv.NewEncoder(cfg.Concurrency, out, slice)
	default:
		return nil, errors.Errorf(`unexpected file type "%s"`, slice.Type)
	}
}
