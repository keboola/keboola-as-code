package middleware

import (
	"context"
	"fmt"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/otel/attribute"
	goa "goa.design/goa/v3/pkg"
)

type Endpoints interface {
	Use(m func(goa.Endpoint) goa.Endpoint)
}

// TraceEndpoints register middleware to enrich the http.server.request span with attributes from a Goa endpoint.
func TraceEndpoints[T Endpoints](endpoints T) T {
	endpoints.Use(func(endpoint goa.Endpoint) goa.Endpoint {
		return func(ctx context.Context, request any) (response any, err error) {
			if span, found := RequestSpan(ctx); found {
				serviceName, _ := ctx.Value(goa.ServiceKey).(string)
				endpointName, _ := ctx.Value(goa.MethodKey).(string)
				resName := endpointName
				if routerData := httptreemux.ContextData(ctx); routerData != nil {
					resName += " " + routerData.Route()
				}
				span.SetAttributes(
					attribute.String("resource.name", resName),
					attribute.String("endpoint.service", serviceName),
					attribute.String("endpoint.name", endpointName),
					attribute.String("endpoint.fullName", fmt.Sprintf("%s.%s", serviceName, endpointName)),
				)
			}
			return endpoint(ctx, request)
		}
	})
	return endpoints
}
