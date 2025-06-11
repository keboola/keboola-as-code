// nolint: gocritic
package main

import (
	"context"
	"net/http"
	"os"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/DataDog/dd-trace-go/v2/profiler"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/entrypoint"
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
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/pprof"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

const (
	ServiceName       = "templates-api"
	ENVPrefix         = "TEMPLATES_"
	ErrorNamePrefix   = "templates."
	ExceptionIdPrefix = "keboola-templates-"
)

func main() {
	entrypoint.Run(run, config.New(), entrypoint.Config{ENVPrefix: ENVPrefix})
}

func run(ctx context.Context, cfg config.Config, _ []string) error {
	// Create logger
	logger := log.NewServiceLogger(os.Stdout, cfg.DebugLog) // nolint:forbidigo

	// Dump configuration, sensitive values are masked
	dump, err := configmap.NewDumper().Dump(cfg).AsJSON(false)
	if err == nil {
		logger.Infof(ctx, "configuration: %s", string(dump))
	} else {
		return err
	}

	// Create process abstraction
	proc := servicectx.New(servicectx.WithLogger(logger))

	// PProf profiler
	if cfg.PProf.Enabled {
		logger.Infof(ctx, `PProf profiler enabled, listening on %q`, cfg.PProf.Listen)
		srv := pprof.NewHTTPServer(cfg.PProf.Listen)
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Errorf(ctx, `PProf HTTP server error: %s`, err)
			}
		}()
		defer func() {
			if err := srv.Close(); err != nil {
				logger.Errorf(ctx, `cannot stop PProf HTTP server: %s`, err)
			}
		}()
	}

	// Datadog profiler
	if cfg.Datadog.Enabled && cfg.Datadog.Profiler.Enabled {
		logger.Infof(ctx, "Datadog profiler enabled")
		if err := profiler.Start(profiler.WithProfileTypes(cfg.Datadog.Profiler.ProfilerTypes()...)); err != nil {
			return err
		}
		defer profiler.Stop()
	}

	// Setup telemetry
	tel, err := telemetry.New(
		func() (trace.TracerProvider, error) {
			if cfg.Datadog.Enabled {
				return datadog.NewTracerProvider(
					logger, proc,
					tracer.WithRuntimeMetrics(),
					tracer.WithSamplingRules(tracer.TraceSamplingRules(tracer.Rule{Rate: 1.0})),
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

	// Create dependencies
	apiScp, err := dependencies.NewAPIScope(ctx, cfg, proc, logger, tel, os.Stdout, os.Stderr) // nolint:forbidigo
	if err != nil {
		return err
	}

	// Create service
	svc, err := service.New(ctx, apiScp)
	if err != nil {
		return err
	}

	// Start HTTP server
	logger.Infof(ctx, "starting Templates API HTTP server, listen-address=%s", cfg.API.Listen)
	err = httpserver.New(ctx, apiScp, httpserver.Config{
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
				logger.Debugf(ctx, "HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
			}
		},
	}).Start(ctx)
	if err != nil {
		return err
	}

	// Wait for the service shutdown
	proc.WaitForShutdown()
	return nil
}
