package middleware

import (
	"fmt"
	"net/http"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	goa "goa.design/goa/v3/pkg"
)

// OpenTelemetryExtractEndpoint register middleware to enrich the http.server.request span with attributes from a Goa endpoint.
func OpenTelemetryExtractEndpoint() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			serviceName, _ := ctx.Value(goa.ServiceKey).(string)
			endpointName, _ := ctx.Value(goa.MethodKey).(string)
			resName := endpointName
			if routerData := httptreemux.ContextData(ctx); routerData != nil {
				resName = routerData.Route() + " " + resName
			}

			if endpointName != "" {
				// Set metrics attributes
				labeler, _ := otelhttp.LabelerFromContext(ctx)
				labeler.Add(attribute.String("endpoint.name", endpointName))

				// Set span attributes
				if span, found := RequestSpan(ctx); found {
					span.SetAttributes(
						attribute.String("resource.name", resName),
						attribute.String("endpoint.service", serviceName),
						attribute.String("endpoint.name", endpointName),
						attribute.String("endpoint.name_full", fmt.Sprintf("%s.%s", serviceName, endpointName)),
					)
				}
			}

			next.ServeHTTP(w, req)
		})
	}
}
