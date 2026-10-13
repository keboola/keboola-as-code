package httpsource

import (
	"io"
	"net/http"
	"strings"

	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ErrorNamePrefix   = "stream.in."
	ExceptionIDPrefix = "keboola-stream-"
)

// responseWriter is bridge between fasthttp handler and standard http handler.
// It is used to re-use error writer from the httpserver package.
type responseWriter struct {
	*fasthttp.RequestCtx
	header http.Header
}

func newErrorHandler(cfg Config, logger log.Logger) func(c *fasthttp.RequestCtx, err error) {
	// Error handler
	errorWriter := newErrorWriter(logger)
	return func(c *fasthttp.RequestCtx, err error) {
		c.Response.Header.Set("Server", ServerHeader)

		attrs := sourceAttrs(string(c.Path()))
		var smallBufferErr *fasthttp.ErrSmallBuffer

		switch {
		// Headers too large - small reader buffer
		case errors.As(err, &smallBufferErr):
			err = svcErrors.NewHeaderTooLargeError(
				errors.Wrapf(err, `request header size is over the maximum %q`, cfg.ReadBufferSize.String()),
			)
		// Body too large
		case errors.Is(err, fasthttp.ErrBodyTooLarge):
			err = svcErrors.NewBodyTooLargeError(
				errors.Wrapf(err, `request body size is over the maximum %q`, cfg.MaxRequestBodySize.String()),
			)
			if size := c.Request.Header.ContentLength(); size > 0 {
				attrs = append(attrs, attribute.Int("http.request.body.size", size))
			}
		// EOF error, for example: client closed connection
		case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
			err = svcErrors.NewIOError(
				errors.Wrap(err, "unexpected end of request"),
			)
		}

		w := newResponseWriter(c)
		errorWriter.WriteWithStatusCode(ctxattr.ContextWith(c, attrs...), w, err)
		w.Finalize()
	}
}

// sourceAttrs returns the project and source IDs of a /stream/... or /otlp/... request path, never the secret.
func sourceAttrs(path string) []attribute.KeyValue {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if !isSourceRoute(parts) {
		return nil
	}
	return []attribute.KeyValue{
		attribute.String("project.id", parts[1]),
		attribute.String("source.id", parts[2]),
	}
}

// isSourceRoute matches only the exact /stream and /otlp route shapes, so a malformed path never logs its secret as an ID.
func isSourceRoute(parts []string) bool {
	switch parts[0] {
	case "stream":
		return len(parts) == 4
	case "otlp":
		return (len(parts) == 6 && parts[4] == "v1") || (len(parts) == 5 && parts[3] == "v1")
	default:
		return false
	}
}

func newErrorWriter(logger log.Logger) httpserver.ErrorWriter {
	return httpserver.NewErrorWriter(logger, ErrorNamePrefix, ExceptionIDPrefix)
}

func newResponseWriter(c *fasthttp.RequestCtx) *responseWriter {
	return &responseWriter{RequestCtx: c, header: make(http.Header)}
}

func (w *responseWriter) Header() http.Header {
	return w.header
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.SetStatusCode(statusCode)
}

func (w *responseWriter) Finalize() {
	for k, v := range w.header {
		if l := len(v); l > 0 {
			w.Response.Header.Set(k, v[l-1])
		}
	}
}
