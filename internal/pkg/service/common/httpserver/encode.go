package httpserver

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
	errorWriter ErrorWriter
}

func NewEncoder(logger log.Logger, errorWriter ErrorWriter) func(ctx context.Context, w http.ResponseWriter) goaHTTP.Encoder {
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
		return w.errorWriter.WriteOrErr(w.ctx, w.writer, err)
	}
	return w.parent.Encode(v)
}
