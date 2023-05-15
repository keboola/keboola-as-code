// Package httpclient provide HTTP client for API calls with support for telemetry and logging.
package httpclient

import (
	"io"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/client/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Config struct {
	userAgent   string
	telemetry   telemetry.Telemetry
	debugWriter io.Writer
	dumpWriter  io.Writer
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

func New(opts ...Option) client.Client {
	// Apply options
	conf := Config{userAgent: "keboola-go-client"}
	for _, o := range opts {
		o(&conf)
	}

	// Force HTTP2 transport
	transport := client.HTTP2Transport()

	// Create client
	cl := client.New().
		WithTelemetry(conf.telemetry.TracerProvider(), conf.telemetry.MeterProvider()).
		WithTransport(transport).
		WithUserAgent(conf.userAgent)

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
