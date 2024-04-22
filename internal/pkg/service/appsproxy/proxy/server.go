package proxy

import (
	"context"
	"net/http"
	"time"

	oautproxylogger "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"go.opentelemetry.io/otel/propagation"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/logging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/approuter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

const (
	requestTimeout          = 30 * time.Second
	readHeaderTimeout       = 10 * time.Second
	gracefulShutdownTimeout = 30 * time.Second
)

func StartServer(ctx context.Context, d dependencies.ServiceScope) error {
	logger := d.Logger()
	cfg := d.Config()

	handler := NewHandler(d)

	// Start HTTP server
	srv := &http.Server{Addr: cfg.API.Listen, Handler: handler, ReadHeaderTimeout: readHeaderTimeout}
	srv.ErrorLog = log.NewStdErrorLogger(d.Logger().WithComponent("http-server"))
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

func NewHandler(d dependencies.ServiceScope) http.Handler {
	// Setup OAuth2Proxy singleton global logger
	loggerWriter := logging.NewLoggerWriter(d.Logger(), "info")
	oautproxylogger.SetOutput(loggerWriter)
	// Cannot separate errors from info because oauthproxy will override its error writer with either
	// the info writer or os.Stderr depending on Logging.ErrToInfo value whenever a new proxy instance is created
	oautproxylogger.SetErrOutput(loggerWriter)

	mux := http.NewServeMux()

	// Register static assets
	d.PageWriter().MountAssets(mux)

	// Register applications router
	mux.Handle("/", approuter.New(d))

	// Wrap handler with middlewares
	middlewareCfg := middleware.NewConfig(
		middleware.WithPropagators(propagation.TraceContext{}),
		// Ignore health checks
		middleware.WithFilter(func(req *http.Request) bool {
			return req.URL.Path != "/health-check" && req.URL.Path != "/robots.txt"
		}),
	)
	return middleware.Wrap(
		mux,
		middleware.ContextTimout(requestTimeout),
		middleware.RequestInfo(),
		middleware.Filter(middlewareCfg),
		middleware.Logger(d.Logger()),
		middleware.OpenTelemetry(
			d.Telemetry().TracerProvider(),
			d.Telemetry().MeterProvider(),
			middlewareCfg,
		),
	)
}
