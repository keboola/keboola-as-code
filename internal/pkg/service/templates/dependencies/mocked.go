package dependencies

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
)

func NewMockedAPIScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (APIScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledEtcdClient())
	mocked := dependencies.NewMocked(t, opts...)

	var err error
	cfg.StorageAPIHost = mocked.StorageAPIHost()
	cfg.PublicAddress, err = url.Parse("https://templates.keboola.local")
	cfg.Etcd = mocked.TestEtcdCredentials()
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	apiScp, err := newAPIScope(mocked.TestContext(), mocked, cfg)
	require.NoError(t, err)

	mocked.DebugLogger().Truncate()
	return apiScp, mocked
}

func NewMockedPublicRequestScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (PublicRequestScope, dependencies.Mocked) {
	t.Helper()
	apiScp, mock := NewMockedAPIScope(t, cfg, opts...)
	pubReqScp := newPublicRequestScope(apiScp, mock)
	mock.DebugLogger().Truncate()
	return pubReqScp, mock
}

func NewMockedProjectRequestScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (ProjectRequestScope, dependencies.Mocked) {
	t.Helper()
	pubReqScp, mock := NewMockedPublicRequestScope(t, cfg, opts...)
	prjReqSp := newProjectRequestScope(pubReqScp, mock)
	mock.DebugLogger().Truncate()
	return prjReqSp, mock
}
