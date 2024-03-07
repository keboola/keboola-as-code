package http

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	requestTimeout          = 30 * time.Second
	readHeaderTimeout       = 10 * time.Second
	gracefulShutdownTimeout = 30 * time.Second
)

func StartServer(ctx context.Context, d dependencies.ServiceScope, router http.Handler) error {
	logger, tel, cfg := d.Logger(), d.Telemetry(), d.Config()

	handler := newHandler(logger, tel, router, cfg.API.PublicURL)

	// Start HTTP server
	srv := &http.Server{Addr: cfg.API.Listen, Handler: handler, ReadHeaderTimeout: readHeaderTimeout}
	proc := d.Process()
	proc.Add(func(shutdown servicectx.ShutdownFn) {
		// Start HTTP server in a separate goroutine.
		logger.Infof(ctx, "HTTP server listening on %q", cfg.API.Listen)
		serverErr := srv.ListenAndServe()         // ListenAndServe blocks while the server is running
		shutdown(context.Background(), serverErr) // nolint: contextcheck // intentionally creating new context for the shutdown operation
	})

	// Register graceful shutdown
	proc.OnShutdown(func(ctx context.Context) {
		// Shutdown gracefully with a timeout.
		ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
		defer cancel()

		logger.Infof(ctx, "shutting down HTTP server at %q", cfg.API.Listen)

		if err := srv.Shutdown(ctx); err != nil {
			logger.Errorf(ctx, `HTTP server shutdown error: %s`, err)
		}
		logger.Info(ctx, "HTTP server shutdown finished")
	})

	return nil
}

func newHandler(logger log.Logger, tel telemetry.Telemetry, router http.Handler, publicURL *url.URL) http.Handler {
	middlewareCfg := middleware.NewConfig(
		middleware.WithPropagators(propagation.TraceContext{}),
		// Ignore health checks
		middleware.WithFilter(func(req *http.Request) bool {
			return req.URL.Path != "/health-check"
		}),
	)

	handler := middleware.Wrap(
		router,
		middleware.ContextTimout(requestTimeout),
		middleware.RequestInfo(),
		middleware.Filter(middlewareCfg),
		appIDMiddleware(publicURL),
		middleware.Logger(logger),
		middleware.OpenTelemetry(tel.TracerProvider(), tel.MeterProvider(), middlewareCfg),
	)

	return handler
}
