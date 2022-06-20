package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	httpMiddleware "goa.design/goa/v3/http/middleware"
	"goa.design/goa/v3/middleware"
	goa "goa.design/goa/v3/pkg"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

func TraceEndpointsMiddleware() func(endpoint goa.Endpoint) goa.Endpoint {
	return func(endpoint goa.Endpoint) goa.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			// Trace all endpoints except health check
			resourceName := fmt.Sprintf("%s.%s", ctx.Value(goa.ServiceKey), ctx.Value(goa.MethodKey))
			if !strings.Contains(resourceName, "HealthCheck") {
				// Start operation
				var span tracer.Span
				span, ctx = tracer.StartSpanFromContext(ctx, "endpoint.request", tracer.ResourceName(resourceName))
				// Finis operation and log internal error
				defer func() {
					// Is internal error?
					if err != nil && errorHttpCode(err) > 499 {
						span.Finish(tracer.WithError(err))
						return
					}
					// No internal error
					span.Finish()
				}()
			}

			// Handle
			response, err = endpoint(ctx, request)
			return response, err
		}
	}
}

func ContextMiddleware(d dependencies.Container, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Generate unique request ID
		requestId := idgenerator.RequestId()
		ctx := context.WithValue(r.Context(), middleware.RequestIDKey, requestId)
		// Include request ID in all log messages
		loggerPrefix := fmt.Sprintf("[requestId=%s]", requestId)
		// Add request ID to headers
		w.Header().Add("X-Request-Id", requestId)
		// Add request ID to DD span
		if span, ok := tracer.SpanFromContext(ctx); ok {
			span.SetTag(ext.ResourceName, r.URL.Path)
			span.SetTag("http.request.id", requestId)
			if host, err := d.StorageApiHost(); err == nil {
				span.SetTag("storage.host", host)
			}
		}

		// Cancel context after request
		ctx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()

		// Add dependencies to the context
		ctx = context.WithValue(ctx, dependencies.CtxKey, d.WithCtx(ctx, cancelFn).WithLoggerPrefix(loggerPrefix))

		// Handle
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func LogMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		logger := r.Context().Value(dependencies.CtxKey).(dependencies.Container).PrefixLogger()

		// Log request
		logger.
			WithAdditionalPrefix("[request]").
			Infof("%s %s %s", r.Method, r.URL.String(), from(r))

		// Capture response
		rw := httpMiddleware.CaptureResponse(w)
		h.ServeHTTP(rw, r)

		// Log response
		logger.
			WithAdditionalPrefix("[response]").
			Infof("status=%d bytes=%d time=%s", rw.StatusCode, rw.ContentLength, time.Since(started).String())
	})
}

// from computes the request client IP.
func from(req *http.Request) string {
	if f := req.Header.Get("X-Forwarded-For"); f != "" {
		return f
	}
	f := req.RemoteAddr
	ip, _, err := net.SplitHostPort(f)
	if err != nil {
		return f
	}
	return ip
}
