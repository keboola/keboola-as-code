package main

import (
	"context"
	"net/http"
	"os"
	"syscall"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/DataDog/dd-trace-go/v2/profiler"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/entrypoint"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/pprof"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ServiceName = "stream"
	ENVPrefix   = "STREAM_"
)

func main() {
	entrypoint.Run(run, config.New(), entrypoint.Config{ENVPrefix: ENVPrefix})
}

func run(ctx context.Context, cfg config.Config, posArgs []string) error {
	// Create logger
	logger := log.NewServiceLogger(os.Stdout, cfg.DebugLog) // nolint:forbidigo

	// Get list of enabled components
	components, err := stream.ParseComponentsList(posArgs)
	if err == nil {
		logger.Infof(ctx, "enabled components: %s", components.String())
	} else {
		return err
	}

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
					tracer.WithGlobalTag("stream.components", components.String()),
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

	// Check max opened files limit
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		logger.Warnf(ctx, `cannot get opened file descriptors limit value: %s`, err)
	} else if limit.Cur < 10000 {
		logger.Warnf(ctx, `opened file descriptors limit is too small: %d`, limit.Cur)
	}

	// Create dependencies
	d, err := dependencies.NewServiceScope(ctx, cfg, proc, logger, tel, os.Stdout, os.Stderr) //nolint:forbidigo
	if err != nil {
		return err
	}

	// Start service components
	if err := stream.StartComponents(ctx, d, cfg, components...); err != nil {
		return err
	}

	// Wait for the service shutdown
	proc.WaitForShutdown()
	return nil
}
