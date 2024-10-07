// nolint: gocritic
package main

import (
	"context"
	"net/http"
	"os"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/entrypoint"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/pprof"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

	err = proxy.StartServer(ctx, d)
	if err != nil {
		return err
	}

	// Wait for the service shutdown
	proc.WaitForShutdown()
	return nil
}
