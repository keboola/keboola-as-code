package dependencies

import (
	"context"
	"io"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// baseScope dependencies container implements BaseScope interface.
type baseScope struct {
	logger     log.Logger
	telemetry  telemetry.Telemetry
	stdout     io.Writer
	stderr     io.Writer
	clock      clock.Clock
	httpClient client.Client
	validator  validator.Validator
	process    *servicectx.Process
}

func NewBaseScope(
	ctx context.Context,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
	clk clock.Clock,
	process *servicectx.Process,
	httpClient client.Client,
) BaseScope {
	return newBaseScope(ctx, logger, tel, stdout, stderr, clk, process, httpClient)
}

func newBaseScope(
	ctx context.Context,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
	clk clock.Clock,
	process *servicectx.Process,
	httpClient client.Client,
) *baseScope {
	_, span := tel.Tracer().Start(ctx, "keboola.go.common.dependencies.NewBaseScope")
	defer span.End(nil)
	return &baseScope{
		logger:     logger,
		telemetry:  tel,
		stdout:     stdout,
		stderr:     stderr,
		clock:      clk,
		process:    process,
		httpClient: httpClient,
		validator:  validator.New(),
	}
}

func (v *baseScope) check() {
	if v == nil {
		panic(errors.New("dependencies base scope is not initialized"))
	}
}

func (v *baseScope) Logger() log.Logger {
	v.check()
	return v.logger
}

func (v *baseScope) Telemetry() telemetry.Telemetry {
	v.check()
	return v.telemetry
}

func (v *baseScope) Clock() clock.Clock {
	v.check()
	return v.clock
}

func (v *baseScope) Process() *servicectx.Process {
	v.check()
	return v.process
}

func (v *baseScope) HTTPClient() client.Client {
	v.check()
	return v.httpClient
}

func (v *baseScope) Validator() validator.Validator {
	v.check()
	return v.validator
}

func (v *baseScope) Stdout() io.Writer {
	v.check()
	return v.stdout
}

func (v *baseScope) Stderr() io.Writer {
	v.check()
	return v.stderr
}
