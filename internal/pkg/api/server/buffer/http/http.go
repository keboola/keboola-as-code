package http

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	goaHTTP "goa.design/goa/v3/http"
	httpMiddleware "goa.design/goa/v3/http/middleware"
	dataDog "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	bufferSvr "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/http/buffer/server"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/openapi"
	commonHttp "github.com/keboola/keboola-as-code/internal/pkg/api/server/common/http"
	swaggerui "github.com/keboola/keboola-as-code/third_party"
)

// HandleHTTPServer starts configures and starts a HTTP server on the given
// URL. It shuts down the server if any error is received in the error channel.
func HandleHTTPServer(ctx context.Context, d dependencies.ForServer, u *url.URL, endpoints *buffer.Endpoints, errCh chan error, logger *log.Logger, debug bool) {
	// Trace endpoint start, finish and error
	endpoints.Use(TraceEndpointsMiddleware(d))

	// Build the service HTTP request multiplexer and configure it to serve
	// HTTP requests to the service endpoints.
	mux := newMuxer(d.Logger())

	// Wrap the endpoints with the transport specific layers. The generated
	// server packages contains code generated from the design which maps
	// the service input and output data structures to HTTP requests and
	// responses.
	docsFs := http.FS(openapi.Fs)
	swaggerUiFs := http.FS(swaggerui.SwaggerFS)
	bufferServer := bufferSvr.New(endpoints, mux, commonHttp.NewDecoder(), commonHttp.NewEncoder(d.Logger(), writeError), errorHandler(), errorFormatter(), docsFs, docsFs, docsFs, docsFs, swaggerUiFs)
	if debug {
		servers := goaHTTP.Servers{bufferServer}
		servers.Use(httpMiddleware.Debug(mux, os.Stdout))
	}

	// Configure the mux.
	bufferSvr.Mount(mux, bufferServer)

	// Wrap the multiplexer with additional middlewares. Middlewares mounted
	// here apply to all the service endpoints.
	var handler http.Handler = mux
	handler = LogMiddleware(d, handler)
	handler = ContextMiddleware(d, handler)
	handler = dataDog.WrapHandler(handler, "buffer-api", "", dataDog.WithIgnoreRequest(func(r *http.Request) bool {
		// Trace all requests except health check
		return r.URL.Path == "/health-check"
	}))

	// Start HTTP server using default configuration, change the code to
	// configure the server as required by your service.
	srv := &http.Server{
		Addr:              u.Host,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	for _, m := range bufferServer.Mounts {
		logger.Printf("HTTP %q mounted on %s %s", m.Method, m.Verb, m.Pattern)
	}

	wg := d.ServerWaitGroup()
	wg.Add(1)
	go func() {
		defer wg.Done()

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
