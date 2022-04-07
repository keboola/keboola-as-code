package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	goaHTTP "goa.design/goa/v3/http"
	httpMiddleware "goa.design/goa/v3/http/middleware"
	dataDog "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	templatesSvr "github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/http/templates/server"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/openapi"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

// handleHTTPServer starts configures and starts a HTTP server on the given
// URL. It shuts down the server if any error is received in the error channel.
func handleHTTPServer(ctx context.Context, wg *sync.WaitGroup, d dependencies.Container, u *url.URL, endpoints *templates.Endpoints, errCh chan error, logger *log.Logger, debug bool) {
	// Trace endpoint start, finish and error
	endpoints.Use(TraceEndpointsMiddleware())

	// Build the service HTTP request multiplexer and configure it to serve
	// HTTP requests to the service endpoints.
	mux := newMuxer()

	// Wrap the endpoints with the transport specific layers. The generated
	// server packages contains code generated from the design which maps
	// the service input and output data structures to HTTP requests and
	// responses.
	docsFs := http.FS(openapi.Fs)
	swaggerUiFs := http.FS(swaggerui.SwaggerFS)
	templatesServer := templatesSvr.New(endpoints, mux, decoder, encoder, errorHandler(), errorFormatter(), docsFs, docsFs, docsFs, docsFs, swaggerUiFs)
	if debug {
		servers := goaHTTP.Servers{templatesServer}
		servers.Use(httpMiddleware.Debug(mux, os.Stdout))
	}

	// Configure the mux.
	templatesSvr.Mount(mux, templatesServer)

	// Wrap the multiplexer with additional middlewares. Middlewares mounted
	// here apply to all the service endpoints.
	var handler http.Handler = mux
	handler = dataDog.WrapHandler(handler, "templates-api", "")
	handler = LogMiddleware(handler)
	handler = ContextMiddleware(d, handler)

	// Start HTTP server using default configuration, change the code to
	// configure the server as required by your service.
	srv := &http.Server{Addr: u.Host, Handler: handler}
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
