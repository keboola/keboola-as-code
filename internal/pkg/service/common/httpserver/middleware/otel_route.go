package middleware

import (
	"net/http"
	"sort"
	"strings"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	redactedRouteParamsCtxKey = ctxKey("redacted-route-params")
	attrRouteParam            = "http.route_param."
	attrResourceName          = "resource.name"
)

// OpenTelemetryExtractRoute middleware adds route and route params to span and metrics attributes.
// The middleware must be registered directly to the httptreemux.ContextMux, it depends on httptreemux.ContextData.
func OpenTelemetryExtractRoute() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Route
			if routerData := httptreemux.ContextData(req.Context()); routerData != nil {
				route := routerData.Route()

				// Set metrics attributes
				labeler, _ := otelhttp.LabelerFromContext(ctx)
				labeler.Add(semconv.HTTPRoute(route))

				// Set span metadata
				span := trace.SpanFromContext(ctx)
				span.SetAttributes(attribute.String(attrResourceName, route), semconv.HTTPRoute(route))
				{
					var attrs []attribute.KeyValue
					redactedRouteParams, _ := ctx.Value(redactedRouteParamsCtxKey).(map[string]struct{})
					for key, value := range routerData.Params() {
						if redactedRouteParams != nil {
							if _, found := redactedRouteParams[strings.ToLower(key)]; found {
								value = maskedValue
							}
						}
						attrs = append(attrs, attribute.String(attrRouteParam+key, value))
					}
					sort.SliceStable(attrs, func(i, j int) bool {
						return attrs[i].Key < attrs[j].Key
					})
					span.SetAttributes(attrs...)
				}
			}

			next.ServeHTTP(w, req)
		})
	}
}
