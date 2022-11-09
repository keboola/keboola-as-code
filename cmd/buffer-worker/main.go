package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pkg/errors"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func main() {
	// Flags.
	debugF := flag.Bool("debug", false, "Enable debug log level.")
	debugHttpF := flag.Bool("debug-http", false, "Log HTTP client request and response bodies.")
	flag.Parse()

	// Setup logger.
	logger := log.NewCliLogger(os.Stdout, os.Stderr, nil, *debugF)

	// Envs.
	envs, err := env.FromOs()
	if err != nil {
		logger.Errorf("cannot load envs: %s", err)
		os.Exit(1)
	}

	// Start worker.
	if err := start(*debugF, *debugHttpF, logger, envs); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

// nolint:unparam
func start(debug, debugHttp bool, logger log.Logger, _ *env.Map) error {
	// Create context.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Create wait group
	wg := sync.WaitGroup{}

	// Log info
	logger.Infof("starting Buffer API WORKER, debug=%t, debug-http=%t", debug, debugHttp)

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

	// nolint:godox
	// TODO worker code
	logger.Info("todo use context in a worker code", ctx.Value("todo use context"))

	// Wait for signal.
	logger.Infof("exiting (%v)", <-errCh)

	// Send cancellation signal to the goroutines.
	cancelFn()

	// Wait for goroutines - graceful shutdown.
	wg.Wait()
	logger.Info("exited")
	return nil
}
