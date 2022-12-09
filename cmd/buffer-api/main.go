package main

import (
	"context"
	"flag"
	"net"
	"net/url"
	"os"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	bufferGen "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	bufferHttp "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/http"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func main() {
	// Flags.
	httpHostF := flag.String("http-host", "0.0.0.0", "HTTP host")
	httpPortF := flag.String("http-port", "8000", "HTTP port")
	debugF := flag.Bool("debug", false, "Enable debug log level.")
	debugHTTPF := flag.Bool("debug-http", false, "Log HTTP client request and response bodies.")
	flag.Parse()

	// Create logger.
	logger := log.NewServiceLogger(os.Stderr, *debugF).AddPrefix("[bufferApi]")

	// Envs.
	envs, err := env.FromOs()
	if err != nil {
		logger.Errorf("cannot load envs: %s", err.Error())
		os.Exit(1)
	}

	// Start DataDog tracer.
	if envs.Get("DATADOG_ENABLED") != "false" {
		tracer.Start(
			tracer.WithServiceName("buffer-api"),
			tracer.WithLogger(telemetry.NewDDLogger(logger)),
			tracer.WithRuntimeMetrics(),
			tracer.WithAnalytics(true),
			tracer.WithDebugMode(envs.Get("DATADOG_DEBUG") == "true"),
		)
		defer tracer.Stop()
	}

	// Start server.
	if err := start(*httpHostF, *httpPortF, *debugF, *debugHTTPF, logger, envs); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func start(host, port string, debug, debugHTTP bool, logger log.Logger, envs *env.Map) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc, err := servicectx.New(ctx, cancel, servicectx.WithLogger(logger))
	if err != nil {
		return err
	}

	logger.Infof("starting Buffer API HTTP server, host=%s, port=%s, debug=%t, debug-http=%t", host, port, debug, debugHTTP)

	// Create dependencies.
	d, err := dependencies.NewServerDeps(ctx, proc, envs, logger, debug, debugHTTP)
	if err != nil {
		return err
	}

	// Wrap the services in endpoints that can be invoked from other services
	// potentially running in different processes.
	endpoints := bufferGen.NewEndpoints(service.New(d))

	// Create server URL.
	serverURL := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}

	// Start HTTP server.
	bufferHttp.HandleHTTPServer(proc, d, serverURL, endpoints, debug)

	proc.WaitForShutdown()
	return nil
}
