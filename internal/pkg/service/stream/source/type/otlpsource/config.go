// Package otlpsource adds native OTLP/HTTP endpoints to the Stream HTTP source.
//
// It decodes OpenTelemetry signals (logs in Phase 1, metrics and traces later),
// flattens each nested record into an ordered JSON map, and dispatches every
// record through the standard sink pipeline via a recordctx.Context.
package otlpsource

type Config struct {
	Enabled bool `configKey:"enabled" configUsage:"Enable native OTLP/HTTP endpoints on the HTTP source."`
}

func NewConfig() Config {
	return Config{
		Enabled: true,
	}
}
