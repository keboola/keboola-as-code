// Package httpclient provide HTTP client for API calls with support for telemetry and logging.
package httpclient

import (
	"io"
	"net/http"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	ddHttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type Config struct {
	userAgent   string
	envs        env.Provider
	debugWriter io.Writer
	dumpWriter  io.Writer
}

type Option func(c *Config)

func WithEnvs(envs env.Provider) Option {
	return func(c *Config) {
		c.envs = envs
	}
}

func WithUserAgent(v string) Option {
	return func(c *Config) {
		c.userAgent = v
	}
}

func WithDebugOutput(w io.Writer) Option {
	return func(c *Config) {
		c.debugWriter = w
	}
}

func WithDumpOutput(w io.Writer) Option {
	return func(c *Config) {
		c.dumpWriter = w
	}
}

func New(opts ...Option) client.Client {
	// Apply options
	conf := Config{userAgent: "keboola-go-client"}
	for _, o := range opts {
		o(&conf)
	}

	// Force HTTP2 transport
	transport := client.HTTP2Transport()

	// DataDog low-level tracing (raw http requests)
	if conf.envs != nil && telemetry.IsDataDogEnabled(conf.envs) {
		transport = ddHttp.WrapRoundTripper(
			transport,
			ddHttp.WithBefore(func(request *http.Request, span ddtrace.Span) {
				// We use "http.request" operation name for request to the API,
				// so requests to other API must have different operation name.
				span.SetOperationName("kac.api.client.http.request")
				span.SetTag("http.host", request.URL.Host)
				if dotPos := strings.IndexByte(request.URL.Host, '.'); dotPos > 0 {
					// E.g. connection, encryption, scheduler ...
					span.SetTag("http.hostPrefix", request.URL.Host[:dotPos])
				}
				span.SetTag(ext.EventSampleRate, 1.0)
				span.SetTag(ext.HTTPURL, request.URL.Redacted())
				span.SetTag("http.path", request.URL.Path)
				span.SetTag("http.query", request.URL.Query().Encode())
			}),
			ddHttp.RTWithResourceNamer(func(r *http.Request) string {
				// Set resource name to request path
				return strhelper.MustURLPathUnescape(r.URL.RequestURI())
			}))
	}

	// Create client
	cl := client.New().
		WithTransport(transport).
		WithUserAgent(conf.userAgent)

	// Log each HTTP client request/response as debug message
	if conf.debugWriter != nil {
		cl = cl.AndTrace(client.LogTracer(conf.debugWriter))
	}

	// Dump each HTTP client request/response body
	if conf.dumpWriter != nil {
		cl = cl.AndTrace(client.DumpTracer(conf.dumpWriter))
	}

	// DataDog high-level tracing (api client requests)
	if conf.envs != nil && telemetry.IsDataDogEnabled(conf.envs) {
		cl = cl.AndTrace(telemetry.DDTraceFactory())
	}

	return cl
}
