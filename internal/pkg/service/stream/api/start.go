package api

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/propagation"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	apiServer "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/http/stream/server"
	streamGen "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/openapi"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

const (
	ErrorNamePrefix   = "stream.api."
	ExceptionIDPrefix = "keboola-stream-api-"
)

func Start(ctx context.Context, d dependencies.APIScope, cfg config.Config) error {
	// Create service
	svc := service.New(d, cfg)

	// Start HTTP server
	return httpserver.Start(ctx, d, httpserver.Config{
		ListenAddress:     cfg.API.Listen,
		ErrorNamePrefix:   ErrorNamePrefix,
		ExceptionIDPrefix: ExceptionIDPrefix,
		MiddlewareOptions: []middleware.Option{
			middleware.WithRedactedRouteParam("secret"),
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
			middleware.WithPropagators(propagation.TraceContext{}),
			// Ignore health checks
			middleware.WithFilter(func(req *http.Request) bool {
				return req.URL.Path != "/health-check"
			}),
		},
		Mount: func(c httpserver.Components) {
			// Create public request deps for each request
			c.Muxer.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					next.ServeHTTP(w, req.WithContext(context.WithValue(
						req.Context(),
						dependencies.PublicRequestScopeCtxKey, dependencies.NewPublicRequestScope(d, req),
					)))
				})
			})

			// Create server with endpoints
			docsFs := http.FS(openapi.Fs)
			swaggerFs := http.FS(swaggerui.SwaggerFS)
			endpoints := streamGen.NewEndpoints(svc)
			endpoints.Use(middleware.OpenTelemetryExtractEndpoint())
			server := apiServer.New(endpoints, c.Muxer, c.Decoder, c.Encoder, c.ErrorHandler, c.ErrorFormatter, docsFs, docsFs, docsFs, docsFs, swaggerFs)

			// Mount endpoints
			server.Mount(c.Muxer)
			for _, m := range server.Mounts {
				c.Logger.Debugf(ctx, "HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
			}
		},
	})
}
