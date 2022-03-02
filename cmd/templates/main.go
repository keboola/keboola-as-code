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

	templatesApi "github.com/keboola/keboola-as-code/internal/pkg/template/api"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
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
	debugF := flag.Bool("debug", false, "Log request and response bodies")
	flag.Parse()

	// Setup logger.
	logger := log.New(os.Stderr, "[templatesApi] ", log.Ltime)

	// Start DataDog tracer.
	tracer.Start(tracer.WithServiceName("templates-api"), tracer.WithLogger(ddLogger{logger}))
	defer tracer.Stop()

	// Initialize the services.
	templatesSvc := templatesApi.NewTemplates(logger)

	// Wrap the services in endpoints that can be invoked from other services
	// potentially running in different processes.
	templatesEndpoints := templates.NewEndpoints(templatesSvc)

	// Create channel used by both the signal handler and server goroutines
	// to notify the main goroutine when to stop the server.
	errc := make(chan error)

	// Setup interrupt handler. This optional step configures the process so
	// that SIGINT and SIGTERM signals cause the services to stop gracefully.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	// Create server URL.
	serverUrl := &url.URL{Scheme: "http", Host: net.JoinHostPort(*httpHostF, *httpPortF)}

	// Start HTTP server.
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	handleHTTPServer(ctx, serverUrl, templatesEndpoints, &wg, errc, logger, *debugF)

	// Wait for signal.
	logger.Printf("exiting (%v)", <-errc)

	// Send cancellation signal to the goroutines.
	cancel()

	// Wait for goroutines.
	wg.Wait()
	logger.Println("exited")
}
