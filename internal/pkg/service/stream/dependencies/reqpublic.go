package dependencies

import (
	"net/http"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// publicRequestScope implements PublicRequestScope interface.
type publicRequestScope struct {
	APIScope
	dependencies.RequestInfo
	logger log.Logger
}

func NewPublicRequestScope(apiScp APIScope, req *http.Request) PublicRequestScope {
	return newPublicRequestScope(apiScp, dependencies.NewRequestInfo(req))
}

func NewMockedPublicRequestScope(t *testing.T, opts ...dependencies.MockedOption) (PublicRequestScope, Mocked) {
	t.Helper()
	apiScp, mock := NewMockedAPIScope(t, opts...)
	pubReqScp := newPublicRequestScope(apiScp, mock)
	mock.DebugLogger().Truncate()
	return pubReqScp, mock
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
