// Package httpsource provides an optimized HTTP server for receiving records via HTTP request.
package httpsource

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/jonboulle/clockwork"
	jsoniter "github.com/json-iterator/go"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	sinkRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/dispatcher"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ServerHeader            = "Keboola/Stream/HTTPSource"
	gracefulShutdownTimeout = 30 * time.Second
)

var (
	// json - replacement of the standard encoding/json library, it is faster for larger responses.
	json                       = jsoniter.ConfigCompatibleWithStandardLibrary //nolint:gochecknoglobals
	contentTypeHeader          = []byte("Content-Type")                       //nolint:gochecknoglobals
	textPlainContentType       = []byte("text/plain")                         //nolint:gochecknoglobals
	applicationJSONContentType = []byte("application/json")                   //nolint:gochecknoglobals
	okResponse                 = []byte("OK")                                 //nolint:gochecknoglobals
)

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	DefinitionRepository() *definitionRepo.Repository
	SinkRouter() *sinkRouter.Router
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

func Start(ctx context.Context, d dependencies, cfg Config) error {
	logger := d.Logger().WithComponent("http-source")
	logger.Info(ctx, "starting HTTP source node")
	errorHandler := newErrorHandler(cfg, logger)

	// Static routes
	router := routing.New()
	router.Use(func(c *routing.Context) error {
		c.Response.Header.Set("Server", ServerHeader)
		return nil
	})
	router.NotFound(routing.MethodNotAllowedHandler, func(c *routing.Context) error {
		errorHandler(c.RequestCtx, svcErrors.NewRouteNotFound(errors.New("not found, please send data using POST /stream/<projectID>/<sourceID>/<secret>")))
		return nil
	})
	router.Get("/health-check", func(c *routing.Context) error {
		c.SuccessString("text/plain", "OK\n")
		return nil
	})

	// Create dispatcher
	dp, err := dispatcher.New(d, logger)
	if err != nil {
		return err
	}

	// Route import requests to the dispatcher
	router.Options("/stream/<projectID>/<sourceID>/<secret>", func(c *routing.Context) error {
		c.Response.Header.Set("Allow", "OPTIONS, POST")
		c.Response.Header.Set("Access-Control-Allow-Methods", "OPTIONS, POST")
		c.Response.Header.Set("Access-Control-Allow-Headers", "*")
		c.Response.Header.Set("Access-Control-Expose-Headers", "*")
		c.Response.Header.Set("Access-Control-Allow-Origin", "*")
		c.Response.SetStatusCode(http.StatusOK)
		return nil
	})
	router.Post("/stream/<projectID>/<sourceID>/<secret>", func(c *routing.Context) error {
		// Get parameters
		projectIDStr := c.Param("projectID")
		projectIDInt, err := strconv.Atoi(projectIDStr)
		if err != nil {
			errorHandler(c.RequestCtx, svcErrors.NewBadRequestError(errors.Errorf("invalid project ID %q", projectIDStr)))
			return nil //nolint:nilerr
		}
		sourceID := key.SourceID(c.Param("sourceID"))
		secret := c.Param("secret")

		// Create record context
		ctx := telemetry.ContextWithDisabledTracing(ctx) // disable spans in the hot path
		recordCtx := recordctx.FromFastHTTP(ctx, d.Clock().Now(), c.RequestCtx)

		// Dispatch request to all sinks
		result, err := dp.Dispatch(keboola.ProjectID(projectIDInt), sourceID, secret, recordCtx)
		if err != nil {
			// Create an enriched context.Context for logging this specific error event.
			errorLoggingCtx := ctxattr.ContextWith(ctx,
				attribute.String("project.id", strconv.Itoa(projectIDInt)),
				attribute.String("source.id", string(sourceID)),
			)
			// Log the detailed error with the enriched context and attributes.
			logger.Warn(errorLoggingCtx, errors.Wrapf(err, "dispatch failed").Error())
			errorHandler(c.RequestCtx, err)
			return nil //nolint:nilerr
		}

		// Write short response, if there is no error, and there is no verbose=true query param
		verbose := string(c.QueryArgs().Peek("verbose"))
		if result.FailedSinks == 0 && verbose != "true" {
			c.Response.Header.SetCanonical(contentTypeHeader, textPlainContentType)
			c.Response.SetStatusCode(result.StatusCode)
			c.Response.SetBody(okResponse)
			return nil
		}

		// Write verbose response
		result.Finalize() // generate messages
		c.Response.Header.SetCanonical(contentTypeHeader, applicationJSONContentType)
		c.Response.SetStatusCode(result.StatusCode)
		enc := json.NewEncoder(c)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			// Create an enriched context.Context for logging this specific error event.
			errorLoggingCtx := ctxattr.ContextWith(ctx,
				attribute.String("project.id", strconv.Itoa(projectIDInt)),
				attribute.String("source.id", string(sourceID)),
			)
			// Log the detailed error with the enriched context and attributes.
			logger.Warn(errorLoggingCtx, errors.Wrapf(err, "dispatch failed").Error())
			errorHandler(c.RequestCtx, err)
			return nil //nolint:nilerr
		}

		return nil
	})

	// Prepare HTTP server
	readBufferSize, err := safecast.ToInt(cfg.ReadBufferSize.Bytes())
	if err != nil {
		return err
	}
	writeBufferSize, err := safecast.ToInt(cfg.WriteBufferSize.Bytes())
	if err != nil {
		return err
	}
	maxRequestBodySize, err := safecast.ToInt(cfg.MaxRequestBodySize.Bytes())
	if err != nil {
		return err
	}

	srv := &fasthttp.Server{
		Handler:                      fasthttp.TimeoutHandler(router.HandleRequest, cfg.RequestTimeout, "request timeout"),
		ErrorHandler:                 errorHandler,
		Concurrency:                  cfg.MaxConnections,
		IdleTimeout:                  cfg.IdleTimeout,
		ReadBufferSize:               readBufferSize,
		WriteBufferSize:              writeBufferSize,
		ReduceMemoryUsage:            true, // ctx.ResetBody frees the buffer for reuse (slightly higher CPU usage)
		MaxRequestBodySize:           maxRequestBodySize,
		StreamRequestBody:            false, // true is slower
		TCPKeepalive:                 true,
		NoDefaultServerHeader:        true,
		DisablePreParseMultipartForm: true,
		NoDefaultDate:                true,
		NoDefaultContentType:         true,
		CloseOnShutdown:              true,
		Logger:                       log.NewStdErrorLogger(log.NewNopLogger()), // errors are handled by the error handler
	}

	// Calling the server shutdown concurrently with the starting the server causes a deadlock.
	// We have to wait for a successful/unsuccessful start of the server.
	startCtx, startDone := context.WithCancelCause(ctx)
	go func() {
		for {
			<-time.After(time.Millisecond)
			if srv.GetOpenConnectionsCount() != -1 {
				startDone(errors.New("HTTP source server started"))
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
			shutdown(context.WithoutCancel(ctx), err)
			return
		}
		// Serve requests
		logger.Infof(ctx, "started HTTP source on %q", cfg.Listen)
		serverErr := srv.Serve(conn) // blocks while the server is running
		// Server finished
		startDone(errors.New("server finished"))
		shutdown(context.WithoutCancel(ctx), serverErr)
	})

	// Register graceful shutdown
	proc.OnShutdown(func(ctx context.Context) {
		<-startCtx.Done()
		logger.Infof(ctx, "shutting down HTTP source at %q", cfg.Listen)

		// Shutdown gracefully with a timeout.
		ctx, cancel := context.WithTimeoutCause(ctx, gracefulShutdownTimeout, errors.New("graceful shutdown timeout"))
		defer cancel()

		// Shutdown HTTP server
		if err := srv.ShutdownWithContext(ctx); err != nil {
			logger.Errorf(ctx, `HTTP source server shutdown error: %s`, err)
		}

		// Close dispatcher, wait for in-flight requests
		if err := dp.Close(ctx); err != nil {
			logger.Errorf(ctx, `HTTP source dispatcher shutdown error: %s`, err)
		}

		logger.Info(ctx, "HTTP source shutdown done")
	})

	return nil
}
