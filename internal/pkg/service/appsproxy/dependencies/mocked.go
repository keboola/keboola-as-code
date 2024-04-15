package dependencies

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// mocked implements Mocked interface.
type mocked struct {
	dependencies.Mocked
	config config.Config
}

func (v *mocked) TestConfig() config.Config {
	return v.config
}

func NewMockedServiceScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	t.Helper()

	commonMock := dependencies.NewMocked(t, opts...)

	// Fill in missing fields
	if cfg.API.PublicURL == nil {
		var err error
		cfg.API.PublicURL, err = url.Parse("https://hub.keboola.local")
		require.NoError(t, err)
	}
	if cfg.CookieSecretSalt == "" {
		cfg.CookieSecretSalt = "foo"
	}
	if cfg.SandboxesAPI.URL == "" {
		cfg.SandboxesAPI.URL = "http://sandboxes-service-api.default.svc.cluster.local"
	}
	if cfg.SandboxesAPI.Token == "" {
		cfg.SandboxesAPI.Token = "my-token"
	}

	// Validate config
	require.NoError(t, cfg.Validate())

	mock := &mocked{Mocked: commonMock, config: cfg}

	scope, err := newServiceScope(mock.TestContext(), mock, cfg)
	require.NoError(t, err)

	mock.DebugLogger().Truncate()
	mock.MockedHTTPTransport().Reset()
	return scope, mock
}
