package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	bufferGen "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	bufferHttp "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/http"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/cpuprofile"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func main() {
	if err := run(); err != nil {
		fmt.Printf("fatal error: %s\n", err.Error()) // nolint:forbidigo
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

	// Start DataDog tracer.
	if cfg.DatadogEnabled {
		tracer.Start(
			tracer.WithLogger(telemetry.NewDDLogger(logger)),
			tracer.WithRuntimeMetrics(),
			tracer.WithSamplingRules([]tracer.SamplingRule{tracer.RateRule(1.0)}),
			tracer.WithAnalyticsRate(1.0),
			tracer.WithDebugMode(cfg.DatadogDebug),
		)
		defer tracer.Stop()
	}

	// Create process abstraction.
	proc, err := servicectx.New(ctx, cancel, servicectx.WithLogger(logger))
	if err != nil {
		return err
	}

	// Create dependencies.
	d, err := dependencies.NewServerDeps(ctx, proc, cfg, envs, logger)
	if err != nil {
		return err
	}

	// Create service.
	logger.Infof("starting Buffer API HTTP server, listen-address=%s, debug=%t, debug-http=%t", cfg.ListenAddress, cfg.Debug, cfg.DebugHTTP)
	srv := service.New(d)

	// Wrap the services in endpoints that can be invoked from other services
	// potentially running in different processes.
	endpoints := bufferGen.NewEndpoints(srv)

	// Start HTTP server.
	bufferHttp.HandleHTTPServer(proc, d, cfg.ListenAddress, endpoints, cfg.Debug)

	// Wait for the service shutdown.
	proc.WaitForShutdown()
	return nil
}
