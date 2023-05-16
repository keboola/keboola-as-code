package middleware

import (
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
)

type otelConfig struct {
	propagators         propagation.TextMapPropagator
	filters             []otelhttp.Filter
	redactedRouteParams map[string]struct{}
	redactedQueryParams map[string]struct{}
	redactedHeaders     map[string]struct{}
}

type OTELOption func(config *otelConfig)

func WithPropagators(v propagation.TextMapPropagator) OTELOption {
	return func(c *otelConfig) {
		c.propagators = v
	}
}

func WithFilter(filters ...otelhttp.Filter) OTELOption {
	return func(c *otelConfig) {
		c.filters = append(c.filters, filters...)
	}
}

func WithRedactedRouteParam(params ...string) OTELOption {
	return func(c *otelConfig) {
		for _, p := range params {
			c.redactedRouteParams[strings.ToLower(p)] = struct{}{}
		}
	}
}

func WithRedactedQueryParam(params ...string) OTELOption {
	return func(c *otelConfig) {
		for _, p := range params {
			c.redactedQueryParams[strings.ToLower(p)] = struct{}{}
		}
	}
}

func WithRedactedHeader(headers ...string) OTELOption {
	return func(c *otelConfig) {
		for _, p := range headers {
			c.redactedHeaders[strings.ToLower(p)] = struct{}{}
		}
	}
}

func newOTELConfig(opts []OTELOption) otelConfig {
	cfg := otelConfig{
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
