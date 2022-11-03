package http

import (
	"context"
	"net/http"

	goaHTTP "goa.design/goa/v3/http"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type encoderWrapper struct {
	ctx         context.Context
	logger      log.Logger
	writer      http.ResponseWriter
	parent      goaHTTP.Encoder
	errorWriter errorWriter
}

type errorWriter func(ctx context.Context, logger log.Logger, w http.ResponseWriter, err error) error

// encoder encodes responses.
func NewEncoder(logger log.Logger, errorWriter errorWriter) func(ctx context.Context, w http.ResponseWriter) goaHTTP.Encoder {
	return func(ctx context.Context, w http.ResponseWriter) goaHTTP.Encoder {
		return encoderWrapper{
			writer:      w,
			ctx:         ctx,
			logger:      logger,
			parent:      goaHTTP.ResponseEncoder(ctx, w),
			errorWriter: errorWriter,
		}
	}
}

func (w encoderWrapper) Encode(v interface{}) error {
	if err, ok := v.(error); ok {
		return w.errorWriter(w.ctx, w.logger, w.writer, err)
	}
	return w.parent.Encode(v)
}
