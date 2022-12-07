package main

import (
	"context"
	"flag"
	"os"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func main() {
	// Flags.
	debugF := flag.Bool("debug", false, "Enable debug log level.")
	debugHTTPF := flag.Bool("debug-http", false, "Log HTTP client request and response bodies.")
	flag.Parse()

	// Create logger.
	logger := log.NewServiceLogger(os.Stderr, *debugF).AddPrefix("[bufferWorker]")

	// Envs.
	envs, err := env.FromOs()
	if err != nil {
		logger.Errorf("cannot load envs: %s", err.Error())
		os.Exit(1)
	}

	// Start DataDog tracer.
	if envs.Get("DATADOG_ENABLED") != "false" {
		tracer.Start(
			tracer.WithServiceName("buffer-service"),
			tracer.WithLogger(telemetry.NewDDLogger(logger)),
			tracer.WithRuntimeMetrics(),
			tracer.WithAnalytics(true),
			tracer.WithDebugMode(envs.Get("DATADOG_DEBUG") == "true"),
		)
		defer tracer.Stop()
	}

	// Start worker.
	if err := start(*debugF, *debugHTTPF, logger, envs); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func start(debug, debugHTTP bool, logger log.Logger, envs *env.Map) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc, err := servicectx.New(ctx, cancel, logger)
	if err != nil {
		return err
	}

	logger.Infof("starting Buffer API WORKER, debug=%t, debug-http=%t", debug, debugHTTP)

	// Create dependencies.
	d, err := dependencies.NewWorkerDeps(ctx, proc, envs, logger, debug, debugHTTP)
	if err != nil {
		return err
	}

	// Start worker code
	service.New(d).Start()

	proc.WaitForShutdown()
	return nil
}
