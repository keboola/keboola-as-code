package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	bufferGen "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	bufferGenSvr "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/http/buffer/server"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/openapi"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/cpuprofile"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

const (
	ServiceName       = "buffer-api"
	ErrorNamePrefix   = "buffer."
	ExceptionIdPrefix = "keboola-buffer-"
)

func main() {
	if err := run(); err != nil {
		fmt.Println(errors.PrefixError(err, "fatal error").Error()) // nolint:forbidigo
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load configuration.
	envs, err := env.FromOs()
	if err != nil {
		return errors.Errorf("cannot load envs: %w", err)
	}
	cfg, err := config.BindAPIConfig(os.Args, envs)
	if errors.Is(err, pflag.ErrHelp) {
		// Stop on --help flag
		return nil
	} else if err != nil {
		return err
	}

	// Create logger.
	logger := log.NewServiceLogger(os.Stderr, cfg.DebugLog).AddPrefix("[bufferApi]")
	logger.InfoCtx(ctx, "Configuration: ", cfg.Dump())

	// Start CPU profiling, if enabled.
	if cfg.CPUProfFilePath != "" {
		stop, err := cpuprofile.Start(ctx, cfg.CPUProfFilePath, logger)
		if err != nil {
			return errors.Errorf(`cannot start cpu profiling: %w`, err)
		}
		defer stop()
	}

	// Create process abstraction.
	proc, err := servicectx.New(ctx, cancel, servicectx.WithLogger(logger), servicectx.WithUniqueID(cfg.UniqueID))
	if err != nil {
		return err
	}

	// Setup telemetry
	tel, err := telemetry.New(
		func() (trace.TracerProvider, error) {
			if cfg.DatadogEnabled {
				return telemetry.NewDDTracerProvider(
					logger, proc,
					tracer.WithRuntimeMetrics(),
					tracer.WithSamplingRules([]tracer.SamplingRule{tracer.RateRule(1.0)}),
					tracer.WithAnalyticsRate(1.0),
					tracer.WithDebugMode(cfg.DatadogDebug),
				), nil
			}
			return nil, nil
		},
		func() (metric.MeterProvider, error) {
			return prometheus.ServeMetrics(ctx, ServiceName, cfg.MetricsListenAddress, logger, proc)
		},
	)
	if err != nil {
		return err
	}

	// Create dependencies.
	apiScp, err := dependencies.NewAPIScope(ctx, cfg, proc, logger, tel)
	if err != nil {
		return err
	}

	// Create service.
	svc := service.New(apiScp)

	filterImportEndpoint := func(req *http.Request) bool {
		return !strings.HasPrefix(req.URL.Path, "/v1/import/") || req.URL.RawQuery == "debug=true"
	}

	// Start HTTP server.
	logger.InfofCtx(ctx, "starting Buffer API HTTP server, listen-address=%s", cfg.ListenAddress)
	err = httpserver.Start(ctx, apiScp, httpserver.Config{
		ListenAddress:     cfg.ListenAddress,
		ErrorNamePrefix:   ErrorNamePrefix,
		ExceptionIDPrefix: ExceptionIdPrefix,
		MiddlewareOptions: []middleware.Option{
			middleware.WithRedactedRouteParam("secret"),
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
			middleware.WithPropagators(propagation.TraceContext{}),
			// Ignore health checks
			middleware.WithFilter(func(req *http.Request) bool {
				return req.URL.Path != "/health-check"
			}),
			// Filter out import endpoint traces and logs, but keep metrics
			middleware.WithFilterTracing(filterImportEndpoint),
			middleware.WithFilterAccessLog(filterImportEndpoint),
		},
		Mount: func(c httpserver.Components) {
			// Create public request deps for each request
			c.Muxer.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					next.ServeHTTP(w, req.WithContext(context.WithValue(
						req.Context(),
						dependencies.PublicRequestScopeCtxKey, dependencies.NewPublicRequestScope(apiScp, req),
					)))
				})
			})

			// Create server with endpoints
			docsFs := http.FS(openapi.Fs)
			swaggerUiFs := http.FS(swaggerui.SwaggerFS)
			endpoints := bufferGen.NewEndpoints(svc)
			endpoints.Use(middleware.OpenTelemetryExtractEndpoint())
			server := bufferGenSvr.New(endpoints, c.Muxer, c.Decoder, c.Encoder, c.ErrorHandler, c.ErrorFormatter, docsFs, docsFs, docsFs, docsFs, swaggerUiFs)

			// Mount endpoints
			server.Mount(c.Muxer)
			for _, m := range server.Mounts {
				logger.DebugfCtx(ctx, "HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
			}
		},
	})
	if err != nil {
		return err
	}

	// Wait for the service shutdown.
	proc.WaitForShutdown()
	return nil
}
