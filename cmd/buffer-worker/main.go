package main

import (
	"context"
	"flag"
	stdLog "log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
)

type ddLogger struct {
	*stdLog.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.Print(msg)
}

func main() {
	// Flags.
	debugF := flag.Bool("debug", false, "Enable debug log level.")
	debugHttpF := flag.Bool("debug-http", false, "Log HTTP client request and response bodies.")
	flag.Parse()

	// Setup logger.
	logger := stdLog.New(os.Stderr, "[bufferWorker]", 0)

	// Envs.
	envs, err := env.FromOs()
	if err != nil {
		logger.Println("cannot load envs: " + err.Error())
		os.Exit(1)
	}

	// Start DataDog tracer.
	if envs.Get("DATADOG_ENABLED") != "false" {
		tracer.Start(
			tracer.WithServiceName("buffer-service"),
			tracer.WithLogger(ddLogger{logger}),
			tracer.WithRuntimeMetrics(),
			tracer.WithAnalytics(true),
			tracer.WithDebugMode(envs.Get("DATADOG_DEBUG") == "true"),
		)
		defer tracer.Stop()
	}

	// Start worker.
	if err := start(*debugF, *debugHttpF, logger, envs); err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}
}

// nolint:unparam
func start(debug, debugHttp bool, stdLogger *stdLog.Logger, envs *env.Map) error {
	// Create context.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Create logger
	logger := log.NewApiLogger(stdLogger, "", debug)
	logger.Infof("starting Buffer API WORKER, debug=%t, debug-http=%t", debug, debugHttp)

	// Create dependencies.
	d, err := dependencies.NewWorkerDeps(ctx, envs, logger, debug, debugHttp)
	if err != nil {
		return err
	}

	// Create channel used by both the signal handler and server goroutines
	// to notify the main goroutine when to stop the worker.
	errCh := make(chan error)

	// Setup interrupt handler. This optional step configures the process so
	// that SIGINT and SIGTERM signals cause the services to stop gracefully.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errCh <- errors.Errorf("%s", <-c)
	}()

	// Start worker code
	service.New(d).Start()

	// Wait for signal.
	logger.Infof("exiting (%v)", <-errCh)

	// Send cancellation signal to the goroutines.
	cancelFn()

	// Wait for goroutines - graceful shutdown.
	d.WorkerWaitGroup().Wait()
	logger.Info("exited")
	return nil
}
