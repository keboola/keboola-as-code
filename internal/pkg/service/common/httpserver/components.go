package httpserver

import (
	"context"

	goaHTTP "goa.design/goa/v3/http"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

type Components struct {
	Muxer
	Decoder        Decoder
	Encoder        Encoder
	ErrorHandler   ErrorHandler
	ErrorFormatter ErrorFormatter
}

func newComponents(cfg Config, logger log.Logger) Components {
	errorWr := NewErrorWriter(logger, cfg.ErrorNamePrefix, cfg.ExceptionIDPrefix)
	errFmt := func(ctx context.Context, err error) goaHTTP.Statuser {
		return errors.WrapWithStatusCode(err, errors.HTTPCodeFrom(err))
	}
	return Components{
		Muxer:          NewMuxer(errorWr),
		Decoder:        NewDecoder(),
		Encoder:        NewEncoder(logger, errorWr),
		ErrorHandler:   errorWr.WriteWithStatusCode,
		ErrorFormatter: errFmt,
	}
}
