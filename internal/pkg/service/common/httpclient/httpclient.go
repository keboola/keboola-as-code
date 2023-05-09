// Package httpclient provide HTTP client for API calls with support for telemetry and logging.
package httpclient

import (
	"io"
	"net/http"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type Config struct {
	userAgent   string
	debugWriter io.Writer
	dumpWriter  io.Writer
}

type Option func(c *Config)

// transport is used to set additional telemetry attributes.
type transport struct {
	transport http.RoundTripper
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

func New(tel telemetry.Telemetry, opts ...Option) client.Client {
	// Apply options
	conf := Config{userAgent: "keboola-go-client"}
	for _, o := range opts {
		o(&conf)
	}

	// Force HTTP2 transport
	t := client.HTTP2Transport()

	// Wrap the transport with telemetry
	t = otelhttp.NewTransport(
		&transport{t},
		otelhttp.WithPublicEndpoint(),
		otelhttp.WithTracerProvider(tel.TracerProvider()),
		otelhttp.WithMeterProvider(tel.MeterProvider()),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return "HTTP " + r.Method + " " + strhelper.MustURLPathUnescape(r.URL.RequestURI())
		}),
	)

	// Create client
	cl := client.New().
		WithTransport(t).
		WithUserAgent(conf.userAgent)

	// Log each HTTP client request/response as debug message
	if conf.debugWriter != nil {
		cl = cl.AndTrace(client.LogTracer(conf.debugWriter))
	}

	// Dump each HTTP client request/response body
	if conf.dumpWriter != nil {
		cl = cl.AndTrace(client.DumpTracer(conf.dumpWriter))
	}

	return cl
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	labeler, _ := otelhttp.LabelerFromContext(r.Context())

	if dotPos := strings.IndexByte(r.URL.Host, '.'); dotPos > 0 {
		// Host prefix, e.g. connection, encryption, scheduler ...
		labeler.Add(attribute.String("http.hostPrefix", r.URL.Host[:dotPos]))
		// Host suffix, e.g. keboola.com
		labeler.Add(attribute.String("http.hostSuffix", strings.TrimLeft(r.URL.Host[dotPos:], ".")))
	}

	return t.transport.RoundTrip(r)
}
