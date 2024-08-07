package dependencies

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// publicRequestScope implements PublicRequestScope interface.
type publicRequestScope struct {
	ServiceScope
	dependencies.RequestInfo
	logger log.Logger
}

func NewPublicRequestScope(apiScp ServiceScope, req *http.Request) PublicRequestScope {
	_, span := apiScp.Telemetry().Tracer().Start(req.Context(), "keboola.go.apps.proxy.api.dependencies.NewPublicRequestScope")
	defer span.End(nil)
	return newPublicRequestScope(apiScp, dependencies.NewRequestInfo(req))
}

func newPublicRequestScope(apiScp ServiceScope, reqInfo dependencies.RequestInfo) *publicRequestScope {
	d := &publicRequestScope{}
	d.ServiceScope = apiScp
	d.RequestInfo = reqInfo
	d.logger = apiScp.Logger()
	return d
}

func (v *publicRequestScope) Logger() log.Logger {
	return v.logger
}
