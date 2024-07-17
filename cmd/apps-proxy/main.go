// nolint: gocritic
package main

import (
	"context"
	"net/http"
	"os"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	appsproxyGen "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
	appsproxyGenSvr "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/http/apps_proxy/server"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/openapi"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/entrypoint"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/cpuprofile"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

const (
	ServiceName       = "apps-proxy"
	ENVPrefix         = "APPS_PROXY_"
	ErrorNamePrefix   = "apps-proxy."
	ExceptionIDPrefix = "keboola-apps-proxy-"
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

	// Start CPU profiling, if enabled
	if cfg.CPUProfFilePath != "" {
		stop, err := cpuprofile.Start(ctx, cfg.CPUProfFilePath, logger)
		if err != nil {
			return errors.Errorf(`cannot start cpu profiling: %w`, err)
		}
		defer stop()
	}

	// Create process abstraction
	proc := servicectx.New(servicectx.WithLogger(logger))

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

	// Create dependencies
	d, err := dependencies.NewServiceScope(ctx, cfg, proc, logger, tel, os.Stdout, os.Stderr) // nolint:forbidigo
	if err != nil {
		return err
	}

	// Create service
	svc, err := service.New(ctx, d)
	if err != nil {
		return err
	}

	logger.Infof(ctx, "starting Apps Proxy server, listen-address=%s", cfg.API.Listen)
	err = httpserver.Start(ctx, d, httpserver.Config{
		ListenAddress:     cfg.API.Listen,
		ErrorNamePrefix:   ErrorNamePrefix,
		ExceptionIDPrefix: ExceptionIDPrefix,
		MiddlewareOptions: []middleware.Option{
			middleware.WithRedactedHeader("X-StorageAPI-Token"),
			middleware.WithPropagators(propagation.TraceContext{}),
			middleware.WithFilter(func(req *http.Request) bool {
				return req.URL.Path != "/health-check" && req.URL.Path != "/robots.txt"
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
			swaggerUIFs := http.FS(swaggerui.SwaggerFS)
			endpoints := appsproxyGen.NewEndpoints(svc)
			endpoints.Use(middleware.OpenTelemetryExtractEndpoint())
			server := appsproxyGenSvr.New(endpoints, c.Muxer, c.Decoder, c.Encoder, c.ErrorHandler, c.ErrorFormatter, docsFs, docsFs, docsFs, docsFs, swaggerUIFs)

			// Mount endpoints
			server.Mount(c.Muxer)
			for _, m := range server.Mounts {
				logger.Debugf(ctx, "HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
			}
		},
	})
	if err != nil {
		return err
	}

	// Wait for the service shutdown
	proc.WaitForShutdown()
	return nil
}
