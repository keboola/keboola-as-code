// nolint: gocritic
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	templatesGen "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	templatesHttp "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/http"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/service"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

type ddLogger struct {
	*log.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.Print(msg)
}

func main() {
	// Flags.
	httpHostF := flag.String("http-host", "0.0.0.0", "HTTP host")
	httpPortF := flag.String("http-port", "8000", "HTTP port")
	repositoryPathF := flag.String("repository-path", "https://github.com/keboola/keboola-as-code-templates.git:api-demo", "Path to default repository")
	debugF := flag.Bool("debug", false, "Log request and response bodies")
	flag.Parse()

	// Setup logger.
	logger := log.New(os.Stderr, "[templatesApi]", 0)

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
		)
		defer tracer.Stop()
	}

	// Start server
	if err := start(*httpHostF, *httpPortF, *repositoryPathF, *debugF, logger, envs); err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}
}

func start(host, port string, repoPath string, debug bool, logger *log.Logger, envs *env.Map) error {
	// Create dependencies.
	d, err := dependencies.NewContainer(context.Background(), repoPath, debug, logger, envs)
	if err != nil {
		return err
	}

	// Log options.
	d.Logger().Infof("starting HTTP server, host=%s, port=%s, debug=%t", host, port, debug)

	// Initialize the service.
	svc, err := service.New(d)
	if err != nil {
		return err
	}

	// Wrap the services in endpoints that can be invoked from other services
	// potentially running in different processes.
	endpoints := templatesGen.NewEndpoints(svc)

	// Create channel used by both the signal handler and server goroutines
	// to notify the main goroutine when to stop the server.
	errCh := make(chan error)

	// Setup interrupt handler. This optional step configures the process so
	// that SIGINT and SIGTERM signals cause the services to stop gracefully.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errCh <- fmt.Errorf("%s", <-c)
	}()

	// Create server URL.
	serverUrl := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}

	// Start HTTP server.
	var wg sync.WaitGroup
	templatesHttp.HandleHTTPServer(d.Ctx(), &wg, d, serverUrl, endpoints, errCh, logger, debug)

	// Wait for signal.
	logger.Printf("exiting (%v)", <-errCh)

	// Send cancellation signal to the goroutines.
	d.CtxCancelFn()()

	// Wait for goroutines.
	wg.Wait()
	logger.Println("exited")
	return nil
}
