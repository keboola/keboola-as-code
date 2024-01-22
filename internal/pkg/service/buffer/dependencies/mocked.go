package dependencies

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func NewMockedServiceScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (ServiceScope, dependencies.Mocked) {
	t.Helper()

	// Complete configuration
	if cfg.NodeID == "" {
		cfg.NodeID = "test-node"
	}
	if cfg.StorageAPIHost == "" {
		cfg.StorageAPIHost = "connection.keboola.local"
	}
	if cfg.API.PublicURL == nil {
		cfg.API.PublicURL, _ = url.Parse("https://stream.keboola.local")
	}

	// Create mocked common dependencies
	opts = append(opts, dependencies.WithEnabledEtcdClient(), dependencies.WithMockedStorageAPIHost(cfg.StorageAPIHost))
	mock := dependencies.NewMocked(t, opts...)

	// Obtain etcd credentials
	cfg.Etcd = mock.TestEtcdConfig()

	// Validate configuration
	require.NoError(t, configmap.ValidateAndNormalize(cfg))

	serviceScp := newServiceScope(mock, cfg)

	mock.DebugLogger().Truncate()
	return serviceScp, mock
}

func NewMockedAPIScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (APIScope, dependencies.Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledTasks())
	serviceScp, mock := NewMockedServiceScope(t, cfg, opts...)

	apiScp := newAPIScope(serviceScp)

	mock.DebugLogger().Truncate()
	return apiScp, mock
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
	pubReqScp, mocked := NewMockedPublicRequestScope(t, cfg, opts...)
	prjReqScp := newProjectRequestScope(pubReqScp, mocked)
	return prjReqScp, mocked
}

func NewMockedDefinitionScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (DefinitionScope, dependencies.Mocked) {
	t.Helper()
	svcScope, mocked := NewMockedServiceScope(t, cfg, opts...)
	return newDefinitionScope(svcScope), mocked
}

func NewMockedTableSinkScope(t *testing.T, cfg config.Config, opts ...dependencies.MockedOption) (TableSinkScope, dependencies.Mocked) {
	t.Helper()
	svcScope, mocked := NewMockedDefinitionScope(t, cfg, opts...)
	backoff := storage.NoRandomizationBackoff()
	d, err := newTableSinkScope(svcScope, backoff)
	require.NoError(t, err)
	return d, mocked
}
