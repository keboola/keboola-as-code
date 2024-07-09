package encoder

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Factory interface {
	NewEncoder(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error)
}

type DefaultFactory struct{}

func (DefaultFactory) NewEncoder(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error) {
	switch slice.Type {
	case model.FileTypeCSV:
		return csv.NewEncoder(cfg.Concurrency, out, slice)
	default:
		return nil, errors.Errorf(`unexpected file type "%s"`, slice.Type)
	}
}

func FactoryFn(fn func(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error)) Factory {
	return factoryFn{Fn: fn}
}

type factoryFn struct {
	Fn func(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error)
}

func (f factoryFn) NewEncoder(cfg Config, out io.Writer, slice *model.Slice) (Encoder, error) {
	return f.Fn(cfg, out, slice)
}
