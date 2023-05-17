package middleware

import (
	"fmt"
	"net/http"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/otel/attribute"
	goa "goa.design/goa/v3/pkg"
)

// TraceEndpoints register middleware to enrich the http.server.request span with attributes from a Goa endpoint.
func TraceEndpoints() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			if span, found := RequestSpan(ctx); found {
				serviceName, _ := ctx.Value(goa.ServiceKey).(string)
				endpointName, _ := ctx.Value(goa.MethodKey).(string)
				resName := endpointName
				if routerData := httptreemux.ContextData(ctx); routerData != nil {
					resName = routerData.Route() + " " + resName
				}
				span.SetAttributes(
					attribute.String("resource.name", resName),
					attribute.String("endpoint.service", serviceName),
					attribute.String("endpoint.name", endpointName),
					attribute.String("endpoint.name_full", fmt.Sprintf("%s.%s", serviceName, endpointName)),
				)
			}
			next.ServeHTTP(w, req)
		})
	}
}
