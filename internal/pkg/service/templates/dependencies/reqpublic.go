package dependencies

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// publicRequestScope implements PublicRequestScope interface.
type publicRequestScope struct {
	APIScope
	dependencies.RequestInfo
	logger     log.Logger
	components dependencies.Lazy[*model.ComponentsMap]
}

func NewPublicRequestScope(apiScp APIScope, req *http.Request) PublicRequestScope {
	_, span := apiScp.Telemetry().Tracer().Start(req.Context(), "keboola.go.templates.api.dependencies.NewPublicRequestScope")
	defer span.End(nil)
	return newPublicRequestScope(apiScp, dependencies.NewRequestInfo(req))
}

func newPublicRequestScope(apiScp APIScope, reqInfo dependencies.RequestInfo) *publicRequestScope {
	d := &publicRequestScope{}
	d.APIScope = apiScp
	d.RequestInfo = reqInfo
	d.logger = apiScp.Logger()
	return d
}

func (v *publicRequestScope) Logger() log.Logger {
	return v.logger
}

func (v *publicRequestScope) Components() *model.ComponentsMap {
	// Use the same version of the components during the entire request
	return v.components.MustInitAndGet(func() *model.ComponentsMap {
		return v.APIScope.Components()
	})
}
