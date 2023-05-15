package httpserver

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
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
	errorFmt := FormatError
	return Components{
		Muxer:          NewMuxer(errorWr),
		Decoder:        NewDecoder(),
		Encoder:        NewEncoder(logger, errorWr),
		ErrorHandler:   errorWr.Write,
		ErrorFormatter: errorFmt,
	}
}
