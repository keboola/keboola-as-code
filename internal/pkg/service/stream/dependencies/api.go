package dependencies

import (
	"context"
	"io"
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// apiSCope implements APIScope interface.
type apiScope struct {
	ServiceScope
	logger    log.Logger
	publicURL *url.URL
}

func NewAPIScope(ctx context.Context, cfg config.Config, proc *servicectx.Process, logger log.Logger, tel telemetry.Telemetry, stdout, stderr io.Writer) (v APIScope, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.stream.dependencies.NewAPIScope")
	defer span.End(&err)
	serviceScp, err := NewServiceScope(ctx, cfg, proc, logger, tel, stdout, stderr)
	return newAPIScope(serviceScp, cfg.API), nil
}

func newAPIScope(svcScope ServiceScope, cfg config.API) APIScope {
	d := &apiScope{}

	d.ServiceScope = svcScope

	d.logger = svcScope.Logger().WithComponent("api")

	d.publicURL = cfg.PublicURL

	return d
}

func (v *apiScope) Logger() log.Logger {
	return v.logger
}

func (v *apiScope) APIPublicURL() *url.URL {
	out, _ := url.Parse(v.publicURL.String()) // clone
	return out
}
