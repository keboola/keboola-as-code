package dependencies

import (
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// base dependencies container implements Base interface.
type base struct {
	envs       env.Provider
	logger     log.Logger
	httpClient client.Client
}

func NewBaseDeps(envs env.Provider, logger log.Logger, httpClient client.Client) Base {
	return newBaseDeps(envs, logger, httpClient)
}

func newBaseDeps(envs env.Provider, logger log.Logger, httpClient client.Client) *base {
	return &base{
		envs:       envs,
		logger:     logger,
		httpClient: httpClient,
	}
}

func (v base) Envs() env.Provider {
	return v.envs
}

func (v base) Logger() log.Logger {
	return v.logger
}

func (v base) HttpClient() client.Client {
	return v.httpClient
}
