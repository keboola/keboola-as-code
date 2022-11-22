package dependencies

import (
	"github.com/keboola/go-client/pkg/client"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// base dependencies container implements Base interface.
type base struct {
	envs       env.Provider
	tracer     trace.Tracer
	logger     log.Logger
	httpClient client.Client
}

func NewBaseDeps(envs env.Provider, tracer trace.Tracer, logger log.Logger, httpClient client.Client) Base {
	return newBaseDeps(envs, tracer, logger, httpClient)
}

func newBaseDeps(envs env.Provider, tracer trace.Tracer, logger log.Logger, httpClient client.Client) *base {
	if tracer == nil {
		// Default no operation tracer
		tracer = telemetry.NewNopTracer()
	}

	return &base{
		envs:       envs,
		tracer:     tracer,
		logger:     logger,
		httpClient: httpClient,
	}
}

func (v base) Envs() env.Provider {
	return v.envs
}

func (v base) Tracer() trace.Tracer {
	return v.tracer
}

func (v base) Logger() log.Logger {
	return v.logger
}

func (v base) HttpClient() client.Client {
	return v.httpClient
}
