package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dimfeld/httptreemux/v5"
	httpMiddleware "goa.design/goa/v3/http/middleware"
	"goa.design/goa/v3/middleware"
	goa "goa.design/goa/v3/pkg"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

const RequestTimeout = 60 * time.Second

func TraceEndpointsMiddleware(serverDeps dependencies.ForServer) func(endpoint goa.Endpoint) goa.Endpoint {
	return func(endpoint goa.Endpoint) goa.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			requestId, _ := ctx.Value(middleware.RequestIDKey).(string)
			serviceName, _ := ctx.Value(goa.ServiceKey).(string)
			endpointName, _ := ctx.Value(goa.MethodKey).(string)
			resourceName := fmt.Sprintf("%s.%s", serviceName, endpointName)

			// Trace all endpoints except health check
			if !strings.Contains(resourceName, "HealthCheck") {
				// Start operation
				var span tracer.Span
				span, ctx = tracer.StartSpanFromContext(ctx, "endpoint.request", tracer.SpanType(ext.SpanTypeWeb), tracer.ResourceName(resourceName))

				// Track info
				span.SetTag("kac.http.request.id", requestId)
				span.SetTag("kac.storage.host", serverDeps.StorageApiHost())
				span.SetTag("kac.endpoint.service", serviceName)
				span.SetTag("kac.endpoint.name", endpointName)
				if routerData := httptreemux.ContextData(ctx); routerData != nil {
					span.SetTag("kac.endpoint.route", routerData.Route())
					for k, v := range routerData.Params() {
						span.SetTag("kac.endpoint.params."+k, v)
					}
				}

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

			// Add dependencies to the context
			reqDeps := dependencies.NewDepsForPublicRequest(serverDeps, ctx, requestId)
			ctx = context.WithValue(ctx, dependencies.ForPublicRequestCtxKey, reqDeps)

			// Handle
			response, err = endpoint(ctx, request)
			return response, err
		}
	}
}

func ContextMiddleware(serverDeps dependencies.ForServer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Generate unique request ID
		requestId := idgenerator.RequestId()
		ctx := context.WithValue(r.Context(), middleware.RequestIDKey, requestId) // nolint:staticcheck // intentionally used the ctx key from external package

		// Add request ID to headers
		w.Header().Add("X-Request-Id", requestId)

		// Update span
		if span, ok := tracer.SpanFromContext(ctx); ok {
			span.SetTag(ext.ResourceName, r.URL.Path)
			span.SetTag(ext.HTTPURL, r.URL.Redacted())
			span.SetTag("http.path", r.URL.Path)
			span.SetTag("http.query", r.URL.Query().Encode())
			span.SetTag("kac.http.request.id", requestId)
			span.SetTag("kac.storage.host", serverDeps.StorageApiHost())
		}

		// Cancel context after request + set timeout
		ctx, cancelFn := context.WithTimeout(ctx, RequestTimeout)
		defer cancelFn()

		// Handle
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func LogMiddleware(d dependencies.ForServer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		logger := d.PrefixLogger()

		// Get request ID
		requestId, _ := r.Context().Value(middleware.RequestIDKey).(string)

		// Log request
		logger.
			WithAdditionalPrefix(fmt.Sprintf("[request][requestId=%s]", requestId)).
			Infof("%s %s %s", r.Method, r.URL.String(), from(r))

		// Capture response
		rw := httpMiddleware.CaptureResponse(w)
		h.ServeHTTP(rw, r)

		// Log response
		logger.
			WithAdditionalPrefix(fmt.Sprintf("[response][requestId=%s]", requestId)).
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
