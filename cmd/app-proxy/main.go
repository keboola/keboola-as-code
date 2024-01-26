// nolint: gocritic
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/http"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/cpuprofile"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ServiceName       = "app-proxy"
	ErrorNamePrefix   = "appproxy."
	ExceptionIDPrefix = "keboola-appproxy-"
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
	logger := log.NewServiceLogger(os.Stdout, cfg.DebugLog).WithComponent("appProxy") // nolint:forbidigo
	logger.Infof(ctx, "Configuration: %s", cfg.Dump())

	// Start CPU profiling, if enabled.
	if cfg.CPUProfFilePath != "" {
		stop, err := cpuprofile.Start(ctx, cfg.CPUProfFilePath, logger)
		if err != nil {
			return errors.Errorf(`cannot start cpu profiling: %w`, err)
		}
		defer stop()
	}

	// Create process abstraction.
	proc, err := servicectx.New(servicectx.WithLogger(logger), servicectx.WithUniqueID(cfg.UniqueID))
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
	scope, err := dependencies.NewServiceScope(ctx, cfg, proc, logger, tel, os.Stdout, os.Stderr) // nolint:forbidigo
	if err != nil {
		return err
	}

	logger.Infof(ctx, "starting App Proxy server, listen-address=%s", cfg.ListenAddress)
	router := http.NewRouter(ctx, scope, []http.DataApp{})
	err = http.StartServer(ctx, scope, router.CreateHandler())
	if err != nil {
		return err
	}

	// Wait for the service shutdown.
	proc.WaitForShutdown()
	return nil
}
