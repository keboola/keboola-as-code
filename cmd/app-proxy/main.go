// nolint: gocritic
package main

import (
	"context"
	"os"

	oautproxylogger "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/http"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/logging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/entrypoint"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/cpuprofile"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ServiceName       = "app-proxy"
	ENVPrefix         = "APP_PROXY_"
	ErrorNamePrefix   = "appproxy."
	ExceptionIDPrefix = "keboola-appproxy-"
)

func main() {
	entrypoint.Run(run, config.New(), entrypoint.Config{ENVPrefix: ENVPrefix})
}

func run(ctx context.Context, cfg config.Config) error {
	// Create logger
	logger := log.NewServiceLogger(os.Stdout, cfg.DebugLog).WithComponent("appProxy") // nolint:forbidigo

	// Dump configuration, sensitive values are masked
	dump, err := configmap.NewDumper().Dump(cfg).AsJSON(false)
	if err == nil {
		logger.Infof(ctx, "Configuration: %s", string(dump))
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

	loggerWriter := logging.NewLoggerWriter(logger, "info")
	oautproxylogger.SetOutput(loggerWriter)
	// Cannot separate errors from info because oauthproxy will override its error writer with either
	// the info writer or os.Stderr depending on Logging.ErrToInfo value whenever a new proxy instance is created
	oautproxylogger.SetErrOutput(loggerWriter)

	// Create dependencies
	scope, err := dependencies.NewServiceScope(ctx, cfg, proc, logger, tel, os.Stdout, os.Stderr) // nolint:forbidigo
	if err != nil {
		return err
	}

	logger.Infof(ctx, "starting App Proxy server, listen-address=%s", cfg.API.Listen)
	router, err := http.NewRouter(scope, ExceptionIDPrefix)
	if err != nil {
		return err
	}

	err = http.StartServer(ctx, scope, router.CreateHandler())
	if err != nil {
		return err
	}

	// Wait for the service shutdown
	proc.WaitForShutdown()
	return nil
}
