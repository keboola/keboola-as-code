package dependencies

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// base dependencies container implements Base interface.
type base struct {
	logger     log.Logger
	telemetry  telemetry.Telemetry
	clock      clock.Clock
	httpClient client.Client
	validator  validator.Validator
	process    *servicectx.Process
}

func NewBaseDeps(logger log.Logger, tel telemetry.Telemetry, process *servicectx.Process, httpClient client.Client) Base {
	return newBaseDeps(logger, tel, clock.New(), process, httpClient)
}

func newBaseDeps(logger log.Logger, tel telemetry.Telemetry, clock clock.Clock, process *servicectx.Process, httpClient client.Client) *base {
	return &base{
		logger:     logger,
		telemetry:  tel,
		clock:      clock,
		process:    process,
		httpClient: httpClient,
		validator:  validator.New(),
	}
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

func (v *base) Process() *servicectx.Process {
	return v.process
}

func (v *base) HTTPClient() client.Client {
	return v.httpClient
}
