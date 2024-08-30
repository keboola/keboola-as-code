package dependencies

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

// apiSCope implements APIScope interface.
type apiScope struct {
	ServiceScope
	dependencies.TaskScope
	logger              log.Logger
	apiPublicURL        *url.URL
	httpSourcePublicURL *url.URL
}

func NewAPIScope(serviceScp ServiceScope, taskScp dependencies.TaskScope, cfg config.Config) (v APIScope, err error) {
	return newAPIScope(serviceScp, taskScp, cfg), nil
}

func NewMockedAPIScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (APIScope, Mocked) {
	tb.Helper()
	return NewMockedAPIScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedAPIScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (APIScope, Mocked) {
	tb.Helper()

	svcScp, mock := NewMockedServiceScopeWithConfig(tb, ctx, modifyConfig, opts...)

	tasksScp, err := dependencies.NewTaskScope(ctx, mock.TestConfig().NodeID, exceptionIDPrefix, svcScp)
	require.NoError(tb, err)

	apiScp := newAPIScope(svcScp, tasksScp, mock.TestConfig())

	mock.DebugLogger().Truncate()
	return apiScp, mock
}

func newAPIScope(svcScope ServiceScope, tasksScp dependencies.TaskScope, cfg config.Config) APIScope {
	d := &apiScope{}

	d.ServiceScope = svcScope

	d.TaskScope = tasksScp

	d.logger = svcScope.Logger().WithComponent("api")

	d.apiPublicURL = cfg.API.PublicURL

	d.httpSourcePublicURL = cfg.Source.HTTP.PublicURL
	return d
}

func (v *apiScope) Logger() log.Logger {
	return v.logger
}

func (v *apiScope) APIPublicURL() *url.URL {
	out, _ := url.Parse(v.apiPublicURL.String()) // clone
	return out
}

func (v *apiScope) HTTPSourcePublicURL() *url.URL {
	out, _ := url.Parse(v.httpSourcePublicURL.String()) // clone
	return out
}
