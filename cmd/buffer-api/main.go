package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	ddotel "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentelemetry"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	bufferGen "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	bufferGenSvr "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/http/buffer/server"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/openapi"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
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
	cfg, err := config.LoadFrom(os.Args, envs)
	if errors.Is(err, pflag.ErrHelp) {
		// Stop on --help flag
		return nil
	} else if err != nil {
		return err
	}

	// Create logger.
	logger := log.NewServiceLogger(os.Stderr, cfg.Debug).AddPrefix("[bufferApi]")

	// Start CPU profiling, if enabled.
	if cfg.CPUProfFilePath != "" {
		stop, err := cpuprofile.Start(cfg.CPUProfFilePath, logger)
		if err != nil {
			return errors.Errorf(`cannot start cpu profiling: %w`, err)
		}
		defer stop()
	}

	// Create process abstraction.
	proc, err := servicectx.New(ctx, cancel, servicectx.WithLogger(logger))
	if err != nil {
		return err
	}

	// Setup telemetry
	tel, err := telemetry.NewTelemetry(
		func() (trace.TracerProvider, error) {
			tracerProvider := ddotel.NewTracerProvider(
				tracer.WithLogger(telemetry.NewDDLogger(logger)),
				tracer.WithRuntimeMetrics(),
				tracer.WithSamplingRules([]tracer.SamplingRule{tracer.RateRule(1.0)}),
				tracer.WithAnalyticsRate(1.0),
				tracer.WithDebugMode(cfg.DatadogDebug),
			)
			proc.OnShutdown(func() {
				if err := tracerProvider.Shutdown(); err != nil {
					logger.Error(err)
				}
			})
			return tracerProvider, nil
		},
		func() (metric.MeterProvider, error) {
			return prometheus.ServeMetrics(ctx, ServiceName, cfg.MetricsListenAddress, logger, proc)
		},
	)
	if err != nil {
		return err
	}

	// Create dependencies.
	d, err := dependencies.NewServerDeps(ctx, proc, cfg, envs, logger, tel)
	if err != nil {
		return err
	}

	// Create service.
	svc := service.New(d)

	// Start HTTP server.
	logger.Infof("starting Buffer API HTTP server, listen-address=%s, debug=%t, debug-http=%t", cfg.ListenAddress, cfg.Debug, cfg.DebugHTTP)
	err = httpserver.Start(d, httpserver.Config{
		ListenAddress:     cfg.ListenAddress,
		ErrorNamePrefix:   ErrorNamePrefix,
		ExceptionIDPrefix: ExceptionIdPrefix,
		TelemetryOptions: []middleware.OTELOption{
			middleware.WithRedactedQueryParam("secret"),
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
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
						dependencies.ForPublicRequestCtxKey, dependencies.NewDepsForPublicRequest(d, req),
					)))
				})
			})

			// Create server with endpoints
			docsFs := http.FS(openapi.Fs)
			swaggerUiFs := http.FS(swaggerui.SwaggerFS)
			endpoints := middleware.TraceEndpoints(bufferGen.NewEndpoints(svc))
			server := bufferGenSvr.New(endpoints, c.Muxer, c.Decoder, c.Encoder, c.ErrorHandler, c.ErrorFormatter, docsFs, docsFs, docsFs, docsFs, swaggerUiFs)

			// Mount endpoints
			server.Mount(c.Muxer)
			for _, m := range server.Mounts {
				logger.Infof("HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
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
