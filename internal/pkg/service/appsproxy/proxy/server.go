package proxy

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"strings"
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

type tracerProviderWrapper struct {
	trace.TracerProvider
}

type tracerWrapper struct {
	trace.Tracer
}

type spanWrapper struct {
	trace.Span
	req *http.Request
}

func newTracerProvider(tp trace.TracerProvider) trace.TracerProvider {
	return &tracerProviderWrapper{TracerProvider: tp}
}

func (tp *tracerProviderWrapper) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	return &tracerWrapper{Tracer: tp.TracerProvider.Tracer(name, options...)}
}

func (t *tracerWrapper) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	req, _ := middleware.Request(ctx) // todo panic
	ctx, span := t.Tracer.Start(ctx, spanName, opts...)
	span = &spanWrapper{Span: span, req: req}
	ctx = trace.ContextWithSpan(ctx, span)
	return ctx, span
}

func (w *spanWrapper) SetStatus(code codes.Code, description string) {
	// example
}

func (w *spanWrapper) End(options ...trace.SpanEndOption) {
	if !strings.HasPrefix(w.req.URL.Path, config.InternalPrefix) {
		// Proxied requests are always OK, regardless of the status code
		w.Span.SetStatus(http.StatusOK, "")
	}
	w.Span.End(options...)
}

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
	loggerWriter := logging.NewLoggerWriter(d.Logger().WithComponent("oauth2proxy"), "info")
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
		// Ignore health checks and robots
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
			newTracerProvider(d.Telemetry().TracerProvider()),
			d.Telemetry().MeterProvider(),
			middlewareCfg,
		),
	)
}
