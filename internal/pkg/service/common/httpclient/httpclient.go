// Package httpclient provide HTTP client for API calls with support for telemetry and logging.
package httpclient

import (
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client/trace"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client/trace/otel"
	"go.opentelemetry.io/contrib/propagators/b3"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Config struct {
	userAgent   string
	telemetry   telemetry.Telemetry
	debugWriter io.Writer
	dumpWriter  io.Writer
	forcedHTTP2 bool
}

type Option func(c *Config)

func WithUserAgent(v string) Option {
	return func(c *Config) {
		c.userAgent = v
	}
}

func WithTelemetry(v telemetry.Telemetry) Option {
	return func(c *Config) {
		c.telemetry = v
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

func WithoutForcedHTTP2() Option {
	return func(c *Config) {
		c.forcedHTTP2 = false
	}
}

func New(opts ...Option) client.Client {
	// Apply options
	conf := Config{userAgent: "keboola-sdk-go-client", forcedHTTP2: true}
	for _, o := range opts {
		o(&conf)
	}

	// Create client
	cl := client.New().
		WithUserAgent(conf.userAgent)

	if conf.forcedHTTP2 {
		cl = cl.WithTransport(client.HTTP2Transport())
	} else {
		cl = cl.WithTransport(client.DefaultTransport())
	}

	// Enable telemetry
	if conf.telemetry != nil {
		cl = cl.WithTelemetry(
			conf.telemetry.TracerProvider(),
			conf.telemetry.MeterProvider(),
			otel.WithRedactedHeaders("X-StorageAPI-Token", "X-KBC-ManageApiToken"),
			otel.WithPropagators(
				// DataDog supports multiple propagations: tracecontext, B3, legacy DataDog, ...
				// W3C tracecontext propagator (propagation.TraceContext{}) is not working with the Storage API dd-trace-php ,
				// so the B3 propagator is used.
				b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)),
			),
		)
	}

	// Log each HTTP client request/response as debug message
	if conf.debugWriter != nil {
		cl = cl.AndTrace(trace.LogTracer(conf.debugWriter))
	}

	// Dump each HTTP client request/response body
	if conf.dumpWriter != nil {
		cl = cl.AndTrace(trace.DumpTracer(conf.dumpWriter))
	}

	return cl
}
