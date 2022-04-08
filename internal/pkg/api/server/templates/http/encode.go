package http

import (
	"context"
	"net/http"

	goaHTTP "goa.design/goa/v3/http"
)

type encoderWrapper struct {
	writer http.ResponseWriter
	ctx    context.Context
	parent goaHTTP.Encoder
}

// encoder encodes responses.
func encoder(ctx context.Context, w http.ResponseWriter) goaHTTP.Encoder {
	return encoderWrapper{
		writer: w,
		ctx:    ctx,
		parent: goaHTTP.ResponseEncoder(ctx, w),
	}
}

func (w encoderWrapper) Encode(v interface{}) error {
	if err, ok := v.(error); ok {
		return writeError(w.ctx, w.writer, err)
	}
	return w.parent.Encode(v)
}
