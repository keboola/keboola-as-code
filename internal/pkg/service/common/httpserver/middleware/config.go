package middleware

import (
	"strings"

	"go.opentelemetry.io/otel/propagation"
)

type Config struct {
	propagators         propagation.TextMapPropagator
	filters             []FilterFn
	accessLogFilters    []FilterFn
	tracingFilters      []FilterFn
	redactedRouteParams map[string]struct{}
	redactedQueryParams map[string]struct{}
	redactedHeaders     map[string]struct{}
}

type Option func(config *Config)

// WithPropagators defines propagators of trace/span IDs to other services.
func WithPropagators(v propagation.TextMapPropagator) Option {
	return func(c *Config) {
		c.propagators = v
	}
}

// WithFilter defines ignored requests that will not be traced, metered and logged. It disables telemetry at all.
// A Filter must return true if the request should be logged/traced.
func WithFilter(filters ...FilterFn) Option {
	return func(c *Config) {
		c.filters = append(c.filters, filters...)
	}
}

// WithFilterAccessLog defines ignored requests that will not be logged.
// A Filter must return true if the request should be logged/traced.
func WithFilterAccessLog(filters ...FilterFn) Option {
	return func(c *Config) {
		c.accessLogFilters = append(c.accessLogFilters, filters...)
	}
}

// WithFilterTracing defines ignored requests that will not be traced.
// A Filter must return true if the request should be logged/traced.
func WithFilterTracing(filters ...FilterFn) Option {
	return func(c *Config) {
		c.tracingFilters = append(c.tracingFilters, filters...)
	}
}

// WithRedactedRouteParam defines route parameters excluded from tracing.
func WithRedactedRouteParam(params ...string) Option {
	return func(c *Config) {
		for _, p := range params {
			c.redactedRouteParams[strings.ToLower(p)] = struct{}{}
		}
	}
}

// WithRedactedQueryParam defines query parameters excluded from tracing.
func WithRedactedQueryParam(params ...string) Option {
	return func(c *Config) {
		for _, p := range params {
			c.redactedQueryParams[strings.ToLower(p)] = struct{}{}
		}
	}
}

// WithRedactedHeader defines headers excluded from tracing.
func WithRedactedHeader(headers ...string) Option {
	return func(c *Config) {
		for _, p := range headers {
			c.redactedHeaders[strings.ToLower(p)] = struct{}{}
		}
	}
}

func NewConfig(opts ...Option) Config {
	cfg := Config{
		redactedRouteParams: make(map[string]struct{}),
		redactedQueryParams: make(map[string]struct{}),
		redactedHeaders: map[string]struct{}{
			"authorization":       {},
			"www-authenticate":    {},
			"proxy-authenticate":  {},
			"proxy-authorization": {},
			"cookie":              {},
			"set-cookie":          {},
		},
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
