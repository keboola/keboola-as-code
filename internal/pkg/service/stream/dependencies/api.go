package dependencies

import (
	"net/url"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

// apiSCope implements APIScope interface.
type apiScope struct {
	ServiceScope
	logger              log.Logger
	apiPublicURL        *url.URL
	httpSourcePublicURL *url.URL
}

func NewAPIScope(serviceScp ServiceScope, cfg config.Config) (v APIScope, err error) {
	return newAPIScope(serviceScp, cfg), nil
}

func NewMockedAPIScope(t *testing.T, opts ...dependencies.MockedOption) (APIScope, Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledTasks())
	serviceScp, mock := NewMockedServiceScope(t, opts...)

	apiScp := newAPIScope(serviceScp, mock.TestConfig())

	mock.DebugLogger().Truncate()
	return apiScp, mock
}

func newAPIScope(svcScope ServiceScope, cfg config.Config) APIScope {
	d := &apiScope{}

	d.ServiceScope = svcScope

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
