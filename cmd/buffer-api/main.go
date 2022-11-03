package main

import (
	"context"
	"flag"
	stdLog "log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/dependencies"
	bufferGen "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	bufferHttp "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/http"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/service"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type ddLogger struct {
	*stdLog.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.Print(msg)
}

func main() {
	// Flags.
	httpHostF := flag.String("http-host", "0.0.0.0", "HTTP host")
	httpPortF := flag.String("http-port", "8000", "HTTP port")
	debugF := flag.Bool("debug", false, "Enable debug log level.")
	debugHttpF := flag.Bool("debug-http", false, "Log HTTP client request and response bodies.")
	flag.Parse()

	// Setup logger.
	logger := stdLog.New(os.Stderr, "[bufferApi]", 0)

	// Envs.
	envs, err := env.FromOs()
	if err != nil {
		logger.Println("cannot load envs: " + err.Error())
		os.Exit(1)
	}

	// Start DataDog tracer.
	if envs.Get("DATADOG_ENABLED") != "false" {
		tracer.Start(
			tracer.WithServiceName("templates-api"),
			tracer.WithLogger(ddLogger{logger}),
			tracer.WithRuntimeMetrics(),
			tracer.WithAnalytics(true),
			tracer.WithDebugMode(envs.Get("DATADOG_DEBUG") == "true"),
		)
		defer tracer.Stop()
	}

	// Start server.
	if err := start(*httpHostF, *httpPortF, *debugF, *debugHttpF, logger, envs); err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}
}

func start(host, port string, debug, debugHttp bool, stdLogger *stdLog.Logger, envs *env.Map) error {
	// Create context.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Create logger.
	logger := log.NewApiLogger(stdLogger, "", debug)
	logger.Infof("starting HTTP server, host=%s, port=%s, debug=%t, debug-http=%t", host, port, debug, debugHttp)

	// Create dependencies.
	d, err := dependencies.NewServerDeps(ctx, envs, logger, debug, debugHttp)
	if err != nil {
		return err
	}

	svc := service.New()

	// Wrap the services in endpoints that can be invoked from other services
	// potentially running in different processes.
	endpoints := bufferGen.NewEndpoints(svc)

	// Create channel used by both the signal handler and server goroutines
	// to notify the main goroutine when to stop the server.
	errCh := make(chan error)

	// Setup interrupt handler. This optional step configures the process so
	// that SIGINT and SIGTERM signals cause the services to stop gracefully.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errCh <- errors.Errorf("%s", <-c)
	}()

	// Create server URL.
	serverUrl := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}

	// Start HTTP server.
	bufferHttp.HandleHTTPServer(ctx, d, serverUrl, endpoints, errCh, stdLogger, debug)

	// Wait for signal.
	logger.Infof("exiting (%v)", <-errCh)

	// Send cancellation signal to the goroutines.
	cancelFn()

	// Wait for goroutines - graceful shutdown.
	d.ServerWaitGroup().Wait()
	logger.Info("exited")
	return nil
}
