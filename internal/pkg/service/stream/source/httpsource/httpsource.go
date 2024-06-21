// Package httpsource provides an optimized HTTP server for receiving records via HTTP request.
package httpsource

import (
	"context"
	"net"
	"time"

	"github.com/benbjohnson/clock"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	gracefulShutdownTimeout = 30 * time.Second
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
}

func Start(ctx context.Context, d dependencies, cfg Config) error {
	logger := d.Logger().WithComponent("http-source")
	logger.Info(ctx, "starting HTTP source node")
	errorHandler := newErrorHandler(cfg, logger)

	// Routing
	router := routing.New()
	router.NotFound(routing.MethodNotAllowedHandler, func(c *routing.Context) error {
		errorHandler(c.RequestCtx, svcErrors.NewRouteNotFound(errors.New("not found, please send data using POST /stream/<sourceID>/<secret>")))
		return nil
	})
	router.Get("/health-check", func(c *routing.Context) error {
		c.SuccessString("text/plain", "OK\n")
		return nil
	})
	router.Post("/stream/<sourceID>/<secret>", func(c *routing.Context) error {
		_ = c.Param("sourceID")
		_ = c.Param("secret")
		c.SuccessString("text/plain", "not implemented\n")
		return nil
	})

	// Prepare HTTP server
	srv := &fasthttp.Server{
		Handler:                      fasthttp.TimeoutHandler(router.HandleRequest, cfg.RequestTimeout, "request timeout"),
		ErrorHandler:                 errorHandler,
		Concurrency:                  cfg.MaxConnections,
		IdleTimeout:                  cfg.IdleTimeout,
		ReadBufferSize:               int(cfg.ReadBufferSize.Bytes()),
		WriteBufferSize:              int(cfg.WriteBufferSize.Bytes()),
		ReduceMemoryUsage:            false, // aggressively reduces memory usage at the cost of higher CPU usage
		MaxRequestBodySize:           int(cfg.MaxRequestBodySize.Bytes()),
		TCPKeepalive:                 true,
		NoDefaultServerHeader:        true,
		DisablePreParseMultipartForm: true,
		NoDefaultDate:                true,
		NoDefaultContentType:         true,
		Logger:                       log.NewStdErrorLogger(log.NewNopLogger()), // errors are handled by the error handler
	}

	// Calling the server shutdown concurrently with the starting the server causes a deadlock.
	// We have to wait for a successful/unsuccessful start of the server.
	startCtx, startDone := context.WithCancel(ctx)
	go func() {
		for {
			<-time.After(time.Millisecond)
			if srv.GetOpenConnectionsCount() != -1 {
				startDone()
				return
			}
		}
	}()

	// Start HTTP server in a separate goroutine.
	proc := d.Process()
	proc.Add(func(shutdown servicectx.ShutdownFn) {
		// Create connection
		conn, err := net.Listen("tcp4", cfg.Listen)
		if err != nil {
			shutdown(context.Background(), err) // nolint: contextcheck // intentionally creating new context for the shutdown operation
			return
		}
		// Serve requests
		logger.Infof(ctx, "started HTTP source on %q", cfg.Listen)
		serverErr := srv.Serve(conn) // blocks while the server is running
		// Server finished
		startDone()
		shutdown(context.Background(), serverErr) // nolint: contextcheck // intentionally creating new context for the shutdown operation
	})

	// Register graceful shutdown
	proc.OnShutdown(func(ctx context.Context) {
		<-startCtx.Done()

		// Shutdown gracefully with a timeout.
		ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
		defer cancel()

		logger.Infof(ctx, "shutting down HTTP source at %q", cfg.Listen)

		if err := srv.ShutdownWithContext(ctx); err != nil {
			logger.Errorf(ctx, `HTTP source shutdown error: %s`, err)
		}
		logger.Info(ctx, "HTTP source shutdown finished")
	})

	return nil
}
