// nolint: gocritic
package main

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"net/http"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	templatesGenSvr "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/http/templates/server"
	templatesGen "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/openapi"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/cpuprofile"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

const (
	ServiceName       = "templates-api"
	ErrorNamePrefix   = "templates."
	ExceptionIdPrefix = "keboola-templates-"
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
	cfg, err := config.Bind(os.Args, envs)
	if errors.Is(err, pflag.ErrHelp) {
		// Stop on --help flag
		return nil
	} else if err != nil {
		return err
	}

	// Create logger.
	logger := log.NewServiceLogger(os.Stderr, cfg.DebugLog).AddPrefix("[templatesApi]")

	// Dump configuration, sensitive values are masked
	dump, err := configmap.NewDumper().Dump(cfg).AsJSON(false)
	if err == nil {
		logger.Info("Configuration: ", string(dump))
	} else {
		return err
	}

	// Start CPU profiling, if enabled.
	if cfg.CPUProfFilePath != "" {
		stop, err := cpuprofile.Start(cfg.CPUProfFilePath, logger)
		if err != nil {
			return errors.Errorf(`cannot start cpu profiling: %w`, err)
		}
		defer stop()
	}

	// Create process abstraction.
	proc, err := servicectx.New(ctx, cancel, servicectx.WithLogger(logger), servicectx.WithUniqueID(cfg.NodeID))
	if err != nil {
		return err
	}

	// Setup telemetry
	tel, err := telemetry.New(
		func() (trace.TracerProvider, error) {
			if cfg.Datadog.Enabled {
				return datadog.NewTracerProvider(
					logger, proc,
					tracer.WithRuntimeMetrics(),
					tracer.WithSamplingRules([]tracer.SamplingRule{tracer.RateRule(1.0)}),
					tracer.WithAnalyticsRate(1.0),
					tracer.WithDebugMode(cfg.Datadog.Debug),
				), nil
			}
			return nil, nil
		},
		func() (metric.MeterProvider, error) {
			return prometheus.ServeMetrics(ctx, cfg.Metrics, logger, proc, ServiceName)
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
	svc, err := service.New(ctx, apiScp)
	if err != nil {
		return err
	}

	// Start HTTP server.
	logger.Infof("starting Templates API HTTP server, listen-address=%s", cfg.API.Listen)
	err = httpserver.Start(apiScp, httpserver.Config{
		ListenAddress:     cfg.API.Listen,
		ErrorNamePrefix:   ErrorNamePrefix,
		ExceptionIDPrefix: ExceptionIdPrefix,
		MiddlewareOptions: []middleware.Option{
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
			middleware.WithPropagators(propagation.TraceContext{}),
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
						dependencies.PublicRequestScopeCtxKey, dependencies.NewPublicRequestScope(apiScp, req),
					)))
				})
			})

			// Create server with endpoints
			docsFs := http.FS(openapi.Fs)
			swaggerUiFs := http.FS(swaggerui.SwaggerFS)
			endpoints := templatesGen.NewEndpoints(svc)
			endpoints.Use(middleware.OpenTelemetryExtractEndpoint())
			server := templatesGenSvr.New(endpoints, c.Muxer, c.Decoder, c.Encoder, c.ErrorHandler, c.ErrorFormatter, docsFs, docsFs, docsFs, docsFs, swaggerUiFs)

			// Mount endpoints
			server.Mount(c.Muxer)
			for _, m := range server.Mounts {
				logger.Debugf("HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
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
