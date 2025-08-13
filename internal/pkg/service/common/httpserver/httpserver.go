package httpserver

import (
	"context"
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	requestTimeout          = 30 * time.Second
	readHeaderTimeout       = 10 * time.Second
	gracefulShutdownTimeout = 30 * time.Second
)

type HTTPServer struct {
	*http.Server
	logger        log.Logger
	proc          *servicectx.Process
	listenAddress string
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	Telemetry() telemetry.Telemetry
}

// New creates new instance of HTTP server that is not running yet.
func New(ctx context.Context, d dependencies, cfg Config) *HTTPServer {
	server := &HTTPServer{
		logger:        d.Logger(),
		proc:          d.Process(),
		listenAddress: cfg.ListenAddress,
	}
	server.logger.Infof(ctx, `starting HTTP server on %q`, server.listenAddress)

	// Create server components
	com := newComponents(cfg, server.logger)

	// Register middlewares
	middlewareCfg := middleware.NewConfig(cfg.MiddlewareOptions...)
	com.Use(middleware.OpenTelemetryExtractRoute())
	tel := d.Telemetry()

	// Build middleware chain
	middlewares := []middleware.Middleware{
		middleware.ContextTimeout(requestTimeout),
		middleware.RequestInfo(),
		middleware.Filter(middlewareCfg),
		middleware.Logger(server.logger),
		middleware.OpenTelemetry(tel.TracerProvider(), tel.MeterProvider(), middlewareCfg),
		middleware.OpenTelemetryApdex(tel.MeterProvider()),
	}

	// Add gzip compression if enabled
	if cfg.EnableGzip {
		middlewares = append(middlewares, middleware.Gzip())
	}

	handler := middleware.Wrap(com.Muxer, middlewares...)
	// Mount endpoints
	cfg.Mount(com)
	server.logger.Infof(ctx, "mounted HTTP endpoints")

	// Prepare HTTP server
	server.Server = &http.Server{
		Addr:              server.listenAddress,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          log.NewStdErrorLogger(d.Logger().WithComponent("http-server")),
	}
	return server
}

// Start HTTP server.
func (h *HTTPServer) Start(ctx context.Context) error {
	// Start HTTP server in a separate goroutine.
	h.proc.Add(func(shutdown servicectx.ShutdownFn) {
		h.logger.Infof(ctx, "started HTTP server on %q", h.listenAddress)
		serverErr := h.ListenAndServe() // ListenAndServe blocks while the server is running
		shutdown(context.WithoutCancel(ctx), serverErr)
	})

	// Register graceful shutdown
	h.proc.OnShutdown(func(ctx context.Context) {
		// Shutdown gracefully with a timeout.
		ctx, cancel := context.WithTimeoutCause(ctx, gracefulShutdownTimeout, errors.New("graceful shutdown timeout"))
		defer cancel()

		h.logger.Infof(ctx, "shutting down HTTP server at %q", h.listenAddress)

		if err := h.Shutdown(ctx); err != nil {
			h.logger.Errorf(ctx, `HTTP server shutdown error: %s`, err)
		}
		h.logger.Info(ctx, "HTTP server shutdown finished")
	})

	return nil
}
