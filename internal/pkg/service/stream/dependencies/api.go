package dependencies

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type ctxKey string

const (
	PublicRequestScopeCtxKey  = ctxKey("PublicRequestScope")
	ProjectRequestScopeCtxKey = ctxKey("ProjectRequestScope")
)

// apiSCope implements APIScope interface.
type apiScope struct {
	ServiceScope
}

func NewAPIScope(ctx context.Context, cfg config.Config, proc *servicectx.Process, logger log.Logger, tel telemetry.Telemetry, stdout, stderr io.Writer) (v APIScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.buffer.api.dependencies.NewAPIScope")
	defer span.End(&err)
	serviceScp, err := NewServiceScope(ctx, cfg, proc, logger, tel, stdout, stderr)
	return newAPIScope(serviceScp), nil
}

func newAPIScope(svcScope ServiceScope) APIScope {
	d := &apiScope{}

	d.ServiceScope = svcScope

	return d
}
