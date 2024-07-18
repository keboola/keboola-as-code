package encoder

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Factory interface {
	NewEncoder(cfg Config, mapping any, out io.Writer) (Encoder, error)
}

type DefaultFactory struct{}

func (DefaultFactory) NewEncoder(cfg Config, mapping any, out io.Writer) (Encoder, error) {
	switch cfg.Type {
	case TypeCSV:
		return csv.NewEncoder(cfg.Concurrency, mapping, out)
	default:
		return nil, errors.Errorf(`unexpected encoder type "%s"`, cfg.Type)
	}
}

func FactoryFn(fn func(cfg Config, mapping any, out io.Writer) (Encoder, error)) Factory {
	return factoryFn{Fn: fn}
}

type factoryFn struct {
	Fn func(cfg Config, mapping any, out io.Writer) (Encoder, error)
}

func (f factoryFn) NewEncoder(cfg Config, mapping any, out io.Writer) (Encoder, error) {
	return f.Fn(cfg, mapping, out)
}
