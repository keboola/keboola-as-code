package dependencies

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// base dependencies container implements Base interface.
type base struct {
	envs       env.Provider
	logger     log.Logger
	validator  validator.Validator
	telemetry  telemetry.Telemetry
	clock      clock.Clock
	httpClient client.Client
}

func NewBaseDeps(envs env.Provider, logger log.Logger, tel telemetry.Telemetry, httpClient client.Client) Base {
	return newBaseDeps(envs, logger, tel, clock.New(), httpClient)
}

func newBaseDeps(envs env.Provider, logger log.Logger, tel telemetry.Telemetry, clock clock.Clock, httpClient client.Client) *base {
	return &base{
		envs:       envs,
		logger:     logger,
		validator:  validator.New(),
		telemetry:  tel,
		clock:      clock,
		httpClient: httpClient,
	}
}

func (v *base) Envs() env.Provider {
	return v.envs
}

func (v *base) Validator() validator.Validator {
	return v.validator
}

func (v *base) Telemetry() telemetry.Telemetry {
	return v.telemetry
}

func (v *base) Logger() log.Logger {
	return v.logger
}

func (v *base) Clock() clock.Clock {
	return v.clock
}

func (v *base) HTTPClient() client.Client {
	return v.httpClient
}
