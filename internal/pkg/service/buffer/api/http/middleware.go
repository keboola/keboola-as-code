package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dimfeld/httptreemux/v5"
	httpMiddleware "goa.design/goa/v3/http/middleware"
	"goa.design/goa/v3/middleware"
	goa "goa.design/goa/v3/pkg"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ip"
)

const RequestTimeout = 60 * time.Second

const httpRequestCtxKey = ctxKey("httpRequest")

type ctxKey string

func TraceEndpointsMiddleware(serverDeps dependencies.ForServer) func(endpoint goa.Endpoint) goa.Endpoint {
	return func(endpoint goa.Endpoint) goa.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			requestId, _ := ctx.Value(middleware.RequestIDKey).(string)
			serviceName, _ := ctx.Value(goa.ServiceKey).(string)
			endpointName, _ := ctx.Value(goa.MethodKey).(string)
			resourceName := fmt.Sprintf("%s.%s", serviceName, endpointName)

			opts := []tracer.StartSpanOption{
				tracer.SpanType(ext.SpanTypeWeb),
				tracer.ResourceName(resourceName),
			}

			// Trace all endpoints except health check
			if strings.Contains(resourceName, "HealthCheck") {
				opts = append(opts, tracer.AnalyticsRate(0.0), tracer.Tag(ext.ManualDrop, true))
			} else {
				opts = append(opts, tracer.AnalyticsRate(1.0), tracer.Tag(ext.ManualKeep, true))
			}

			// Create endpoint span
			var span tracer.Span
			span, ctx = tracer.StartSpanFromContext(ctx, "endpoint.request", opts...)

			// Track info
			span.SetTag("keboola.storage.host", serverDeps.StorageAPIHost())
			span.SetTag("http.request.id", requestId)
			span.SetTag("endpoint.service", serviceName)
			span.SetTag("endpoint.name", endpointName)
			if routerData := httptreemux.ContextData(ctx); routerData != nil {
				span.SetTag("endpoint.route", routerData.Route())
				for k, v := range routerData.Params() {
					// Skip the secret parameter
					if k != "secret" {
						span.SetTag("endpoint.params."+k, v)
					}
				}
			}

			// Finis operation and log internal error
			defer func() {
				// Is internal error?
				if err != nil && serviceError.HTTPCodeFrom(err) > 499 {
					span.Finish(tracer.WithError(err))
					return
				}
				// No internal error
				span.Finish()
			}()

			// Add dependencies to the context
			httpRequest := ctx.Value(httpRequestCtxKey).(*http.Request)
			reqDeps := dependencies.NewDepsForPublicRequest(serverDeps, ctx, requestId, httpRequest)
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
		requestId := idgenerator.RequestID()
		ctx := r.Context()
		ctx = context.WithValue(ctx, middleware.RequestIDKey, requestId) // nolint:staticcheck // intentionally used the ctx key from external package
		ctx = context.WithValue(ctx, httpRequestCtxKey, r)

		// Add request ID to headers
		w.Header().Add("X-Request-Id", requestId)

		// Update span
		if span, ok := tracer.SpanFromContext(ctx); ok {
			// Mask the secret parameter in the url
			url := r.URL
			if routerData := httptreemux.ContextData(ctx); routerData != nil {
				for k, v := range routerData.Params() {
					if k == "secret" {
						url.Path = strings.Replace(url.Path, v, "*****", 1)
					}
				}
			}

			span.SetTag(ext.ResourceName, url.Path)
			span.SetTag(ext.HTTPURL, url.Redacted())
			span.SetTag("keboola.storage.host", serverDeps.StorageAPIHost())
			span.SetTag("http.path", url.Path)
			span.SetTag("http.query", r.URL.Query().Encode())
			span.SetTag("http.request.id", requestId)
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
		logger := d.Logger()

		// Get request ID
		requestId, _ := r.Context().Value(middleware.RequestIDKey).(string)

		// Log request
		logger.
			AddPrefix(fmt.Sprintf("[request][requestId=%s]", requestId)).
			Infof("%s %s %s", r.Method, log.Sanitize(r.URL.String()), log.Sanitize(ip.From(r).String()))

		// Capture response
		rw := httpMiddleware.CaptureResponse(w)
		h.ServeHTTP(rw, r)

		// Log response
		logger.
			AddPrefix(fmt.Sprintf("[response][requestId=%s]", requestId)).
			Infof("status=%d bytes=%d time=%s", rw.StatusCode, rw.ContentLength, time.Since(started).String())
	})
}
