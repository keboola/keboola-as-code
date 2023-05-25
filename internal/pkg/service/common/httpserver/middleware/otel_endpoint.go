package middleware

import (
	"context"
	"fmt"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	goa "goa.design/goa/v3/pkg"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// OpenTelemetryExtractEndpoint register middleware to enrich the http.server.request span with attributes from a Goa endpoint.
func OpenTelemetryExtractEndpoint() GoaMiddleware {
	return func(next goa.Endpoint) goa.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			serviceName, _ := ctx.Value(goa.ServiceKey).(string)
			endpointName, _ := ctx.Value(goa.MethodKey).(string)
			resName := endpointName
			if routerData := httptreemux.ContextData(ctx); routerData != nil {
				resName = routerData.Route() + " " + resName
			}

			if endpointName != "" {
				// Set metrics attributes
				labeler, _ := otelhttp.LabelerFromContext(ctx)
				labeler.Add(attribute.String("endpoint.name", strhelper.NormalizeName(endpointName))) // normalize name: prometheus converts string to lower

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

			return next(ctx, req)
		}
	}
}
