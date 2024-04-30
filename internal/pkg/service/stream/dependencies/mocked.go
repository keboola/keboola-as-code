package dependencies

import (
	"net/url"
	"strings"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// mocked implements Mocked interface.
type mocked struct {
	dependencies.Mocked
	config config.Config
}

func (v *mocked) TestConfig() config.Config {
	return v.config
}

func NewMockedServiceScope(t *testing.T, opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	t.Helper()
	return NewMockedServiceScopeWithConfig(t, nil, opts...)
}

func NewMockedServiceScopeWithConfig(t *testing.T, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	t.Helper()

	// Create common mocked dependencies
	commonMock := dependencies.NewMocked(t, append(
		[]dependencies.MockedOption{
			dependencies.WithEnabledEtcdClient(),
			dependencies.WithEnabledDistribution(),
			dependencies.WithEnabledDistributedLocks(),
			dependencies.WithMockedStorageAPIHost("connection.keboola.local"),
		},
		opts...,
	)...)

	// Get and modify test config
	cfg := testConfig(t, commonMock)
	if modifyConfig != nil {
		modifyConfig(&cfg)
	}

	// Create service mocked dependencies
	mock := &mocked{Mocked: commonMock, config: cfg}

	backoff := model.NoRandomizationBackoff()
	serviceScp := newServiceScope(mock, cfg, backoff)

	mock.DebugLogger().Truncate()
	mock.MockedHTTPTransport().Reset()
	return serviceScp, mock
}

func NewMockedAPIScope(t *testing.T, opts ...dependencies.MockedOption) (APIScope, Mocked) {
	t.Helper()

	opts = append(opts, dependencies.WithEnabledTasks())
	serviceScp, mock := NewMockedServiceScope(t, opts...)

	apiScp := newAPIScope(serviceScp, mock.TestConfig())

	mock.DebugLogger().Truncate()
	return apiScp, mock
}

func NewMockedPublicRequestScope(t *testing.T, opts ...dependencies.MockedOption) (PublicRequestScope, Mocked) {
	t.Helper()
	apiScp, mock := NewMockedAPIScope(t, opts...)
	pubReqScp := newPublicRequestScope(apiScp, mock)
	mock.DebugLogger().Truncate()
	return pubReqScp, mock
}

func NewMockedProjectRequestScope(t *testing.T, opts ...dependencies.MockedOption) (ProjectRequestScope, Mocked) {
	t.Helper()
	pubReqScp, mock := NewMockedPublicRequestScope(t, opts...)
	prjReqScp := newProjectRequestScope(pubReqScp, mock)
	return prjReqScp, mock
}

func NewMockedBranchRequestScope(t *testing.T, branchInput key.BranchIDOrDefault, opts ...dependencies.MockedOption) (ProjectRequestScope, Mocked) {
	t.Helper()
	prjReqScp, mocked := NewMockedProjectRequestScope(t, opts...)
	branchReqScp, err := newBranchRequestScope(mocked.TestContext(), prjReqScp, branchInput)
	require.NoError(t, err)
	return branchReqScp, mocked
}

func NewMockedTableSinkScope(t *testing.T, opts ...dependencies.MockedOption) (TableSinkScope, Mocked) {
	t.Helper()
	return NewMockedTableSinkScopeWithConfig(t, nil, opts...)
}

func NewMockedTableSinkScopeWithConfig(t *testing.T, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (TableSinkScope, Mocked) {
	t.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(t, modifyConfig, opts...)
	cfg := mock.TestConfig()
	d, err := newTableSinkScope(tableSinkParentScopesImpl{
		ServiceScope:         svcScp,
		DistributionScope:    mock,
		DistributedLockScope: mock,
	}, cfg)
	require.NoError(t, err)
	return d, mock
}

func testConfig(t *testing.T, d dependencies.Mocked) config.Config {
	t.Helper()
	cfg := config.New()

	// Complete configuration
	cfg.NodeID = "test-node"
	cfg.StorageAPIHost = strings.TrimPrefix(d.StorageAPIHost(), "https://")
	cfg.API.PublicURL, _ = url.Parse("https://stream.keboola.local")
	cfg.Source.HTTP.PublicURL, _ = url.Parse("https://stream-in.keboola.local")
	cfg.Etcd = d.TestEtcdConfig()

	// There are some timers with a few seconds interval.
	// It causes problems when mocked clock is used.
	// For example clock.Add(time.Hour) invokes the timer 3600 times, if the interval is 1s.
	if _, ok := d.Clock().(*clock.Mock); ok {
		cfg.Storage.Statistics.Cache.L2.Enabled = false
	}

	// Validate configuration
	require.NoError(t, configmap.ValidateAndNormalize(&cfg))

	return cfg
}
