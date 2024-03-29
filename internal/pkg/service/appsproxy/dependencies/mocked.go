package dependencies

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func NewMockedServiceScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (ServiceScope, dependencies.Mocked) {
	t.Helper()

	mocked := dependencies.NewMocked(t, opts...)

	var err error
	cfg.API.PublicURL, err = url.Parse("https://hub.keboola.local")
	require.NoError(t, err)

	// Validate config
	require.NoError(t, cfg.Validate())

	scope, err := newServiceScope(mocked.TestContext(), mocked, cfg)
	require.NoError(t, err)

	mocked.DebugLogger().Truncate()
	return scope, mocked
}
