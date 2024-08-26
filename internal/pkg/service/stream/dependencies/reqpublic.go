package dependencies

import (
	"context"
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

func NewMockedPublicRequestScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (PublicRequestScope, Mocked) {
	tb.Helper()
	apiScp, mock := NewMockedAPIScope(tb, ctx, opts...)
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
