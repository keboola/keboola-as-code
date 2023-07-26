package dependencies

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func NewMockedServiceScope(t *testing.T, cfg config.ServiceConfig, opts ...dependencies.MockedOption) (ServiceScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledEtcdClient())
	mock := dependencies.NewMocked(t, opts...)

	cfg.StorageAPIHost = mock.StorageAPIHost()
	cfg.Etcd = mock.TestEtcdCredentials()
	require.NoError(t, cfg.Validate())

	serviceScp, err := newServiceScope(mock, cfg)
	require.NoError(t, err)

	mock.DebugLogger().Truncate()
	return serviceScp, mock
}

func NewMockedWorkerScope(t *testing.T, cfg config.WorkerConfig, opts ...dependencies.MockedOption) (WorkerScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledTasks(), dependencies.WithEnabledDistribution())
	serviceScp, mock := NewMockedServiceScope(t, cfg.ServiceConfig, opts...)

	cfg.ServiceConfig = serviceScp.ServiceConfig()
	require.NoError(t, cfg.Validate())

	workerScp, err := newWorkerScope(mock.TestContext(), cfg, serviceScp)
	require.NoError(t, err)

	mock.DebugLogger().Truncate()
	return workerScp, mock
}

func NewMockedAPIScope(t *testing.T, cfg config.APIConfig, opts ...dependencies.MockedOption) (APIScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledTasks())
	serviceScp, mock := NewMockedServiceScope(t, cfg.ServiceConfig, opts...)

	cfg.ServiceConfig = serviceScp.ServiceConfig()
	cfg.PublicAddress, _ = url.Parse("https://buffer.keboola.local")
	require.NoError(t, cfg.Validate())

	apiScp, err := newAPIScope(cfg, serviceScp)
	require.NoError(t, err)

	mock.DebugLogger().Truncate()
	return apiScp, mock
}

func NewMockedPublicRequestScope(t *testing.T, cfg config.APIConfig, opts ...dependencies.MockedOption) (PublicRequestScope, dependencies.Mocked) {
	t.Helper()
	apiScp, mock := NewMockedAPIScope(t, cfg, opts...)
	pubReqScp := newPublicRequestScope(apiScp, mock)
	mock.DebugLogger().Truncate()
	return pubReqScp, mock
}

func NewMockedProjectRequestScope(t *testing.T, cfg config.APIConfig, opts ...dependencies.MockedOption) (ProjectRequestScope, dependencies.Mocked) {
	t.Helper()
	pubReqScp, mocked := NewMockedPublicRequestScope(t, cfg, opts...)
	prjReqScp := newProjectRequestScope(pubReqScp, mocked)
	return prjReqScp, mocked
}
