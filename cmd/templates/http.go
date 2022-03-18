package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	goaHTTP "goa.design/goa/v3/http"
	httpMiddleware "goa.design/goa/v3/http/middleware"
	"goa.design/goa/v3/middleware"
	dataDog "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	api "github.com/keboola/keboola-as-code/api/templates/gen"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	templatesSvr "github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/http/templates/server"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

// handleHTTPServer starts configures and starts a HTTP server on the given
// URL. It shuts down the server if any error is received in the error channel.
func handleHTTPServer(ctx context.Context, wg *sync.WaitGroup, d dependencies.Container, u *url.URL, endpoints *templates.Endpoints, errCh chan error, logger *log.Logger, debug bool) {
	// Provide the transport specific request decoder and response encoder.
	// The goa http package has built-in support for JSON, XML and gob.
	// Other encodings can be used by providing the corresponding functions,
	// see goa.design/implement/encoding.
	dec := goaHTTP.RequestDecoder
	enc := goaHTTP.ResponseEncoder

	// Build the service HTTP request multiplexer and configure it to serve
	// HTTP requests to the service endpoints.
	mux := goaHTTP.NewMuxer()

	// Wrap the endpoints with the transport specific layers. The generated
	// server packages contains code generated from the design which maps
	// the service input and output data structures to HTTP requests and
	// responses.

	eh := errorHandler(logger)
	docsFS := http.FS(api.Fs)
	swaggerFS := http.FS(swaggerui.SwaggerFS)
	templatesServer := templatesSvr.New(endpoints, mux, dec, enc, eh, nil, docsFS, docsFS, docsFS, docsFS, swaggerFS)
	if debug {
		servers := goaHTTP.Servers{templatesServer}
		servers.Use(httpMiddleware.Debug(mux, os.Stdout))
	}

	// Configure the mux.
	templatesSvr.Mount(mux, templatesServer)

	// Wrap the multiplexer with additional middlewares. Middlewares mounted
	// here apply to all the service endpoints.
	var handler http.Handler = mux
	handler = httpMiddleware.Log(middleware.NewLogger(logger))(handler)
	handler = httpMiddleware.RequestID()(handler)
	handler = dataDog.WrapHandler(handler, "templates-api", "")

	// Start HTTP server using default configuration, change the code to
	// configure the server as required by your service.
	requestCtx := context.WithValue(context.Background(), dependencies.CtxKey, d)
	ctxProvider := func(_ net.Listener) context.Context { return requestCtx }
	srv := &http.Server{Addr: u.Host, Handler: handler, BaseContext: ctxProvider}
	for _, m := range templatesServer.Mounts {
		logger.Printf("HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
	}

	(*wg).Add(1)
	go func() {
		defer (*wg).Done()

		// Start HTTP server in a separate goroutine.
		go func() {
			logger.Printf("HTTP server listening on %q", u.Host)
			errCh <- srv.ListenAndServe()
		}()

		<-ctx.Done()
		logger.Printf("shutting down HTTP server at %q", u.Host)

		// Shutdown gracefully with a 30s timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = srv.Shutdown(ctx)
	}()
}

// errorHandler returns a function that writes and logs the given error.
// The function also writes and logs the error unique ID so that it's possible
// to correlate.
func errorHandler(logger *log.Logger) func(context.Context, http.ResponseWriter, error) {
	return func(ctx context.Context, w http.ResponseWriter, err error) {
		id := ctx.Value(middleware.RequestIDKey).(string)
		_, _ = w.Write([]byte("[" + id + "] encoding: " + err.Error()))
		logger.Printf("[%s] ERROR: %s", id, err.Error())
	}
}
