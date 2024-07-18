package dependencies

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

// mocked implements Mocked interface.
type mocked struct {
	dependencies.Mocked
	config             config.Config
	sinkPipelineOpener *pipeline.TestOpener
}

func (v *mocked) TestConfig() config.Config {
	return v.config
}

func (v *mocked) TestSinkPipelineOpener() *pipeline.TestOpener {
	return v.sinkPipelineOpener
}

func NewMockedServiceScope(t *testing.T, opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	t.Helper()
	return NewMockedServiceScopeWithConfig(t, nil, opts...)
}

func NewMockedServiceScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	tb.Helper()

	// Create common mocked dependencies
	commonMock := dependencies.NewMocked(tb, append(
		[]dependencies.MockedOption{
			dependencies.WithEnabledEtcdClient(),
			dependencies.WithEnabledDistribution(),
			dependencies.WithEnabledDistributedLocks(),
			dependencies.WithMockedStorageAPIHost("connection.keboola.local"),
		},
		opts...,
	)...)

	// Get and modify test config
	cfg := testConfig(tb, commonMock)
	if modifyConfig != nil {
		modifyConfig(&cfg)
	}

	// Create service mocked dependencies
	mock := &mocked{Mocked: commonMock, config: cfg, sinkPipelineOpener: pipeline.NewTestOpener()}

	backoff := model.NoRandomizationBackoff()
	serviceScp, err := newServiceScope(mock, cfg, backoff)
	require.NoError(tb, err)

	mock.DebugLogger().Truncate()
	mock.MockedHTTPTransport().Reset()

	// Register dummy sink with local storage support for tests
	serviceScp.Plugins().RegisterSinkWithLocalStorage(func(sinkType definition.SinkType) bool {
		return sinkType == test.SinkTypeWithLocalStorage
	})
	serviceScp.Plugins().Collection().OnFileOpen(func(ctx context.Context, now time.Time, sink definition.Sink, file *model.File) error {
		if sink.Type == test.SinkTypeWithLocalStorage {
			// Set required fields
			file.Mapping = table.Mapping{Columns: column.Columns{column.Body{Name: "body"}}}
			file.StagingStorage.Provider = "test"
			file.TargetStorage.Provider = "test"
		}
		return nil
	})

	// Register dummy pipeline opener for tests
	serviceScp.Plugins().RegisterSinkPipelineOpener(func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType) (pipeline.Pipeline, error) {
		if sinkType == test.SinkType {
			return mock.sinkPipelineOpener.OpenPipeline()
		}

		return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
	})

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

func NewMockedSourceScope(tb testing.TB, opts ...dependencies.MockedOption) (SourceScope, Mocked) {
	tb.Helper()
	return NewMockedSourceScopeWithConfig(tb, nil, opts...)
}

func NewMockedSourceScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (SourceScope, Mocked) {
	tb.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(
		tb,
		modifyConfig,
		append([]dependencies.MockedOption{dependencies.WithEnabledDistribution()}, opts...)...,
	)
	d, err := newSourceScope(sourceParentScopesImpl{
		ServiceScope:      svcScp,
		DistributionScope: mock,
	}, "test-source", mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func NewMockedLocalStorageScope(t *testing.T, opts ...dependencies.MockedOption) (LocalStorageScope, Mocked) {
	t.Helper()
	return NewMockedLocalStorageScopeWithConfig(t, nil, opts...)
}

func NewMockedLocalStorageScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (LocalStorageScope, Mocked) {
	tb.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(tb, modifyConfig, opts...)
	d, err := newLocalStorageScope(localStorageParentScopesImpl{
		ServiceScope:         svcScp,
		DistributionScope:    mock,
		DistributedLockScope: mock,
	}, mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func testConfig(tb testing.TB, d dependencies.Mocked) config.Config {
	tb.Helper()
	cfg := config.New()

	// Create empty volumes dir
	volumesPath := tb.TempDir()

	// Complete configuration
	cfg.NodeID = "test-node"
	cfg.Hostname = "hostname"
	cfg.StorageAPIHost = strings.TrimPrefix(d.StorageAPIHost(), "https://")
	cfg.Storage.VolumesPath = volumesPath
	cfg.API.PublicURL, _ = url.Parse("https://stream.keboola.local")
	cfg.Source.HTTP.PublicURL, _ = url.Parse("https://stream-in.keboola.local")
	cfg.Etcd = d.TestEtcdConfig()
	cfg.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(tb))

	// There are some timers with a few seconds interval.
	// It causes problems when mocked clock is used.
	// For example clock.Add(time.Hour) invokes the timer 3600 times, if the interval is 1s.
	if _, ok := d.Clock().(*clock.Mock); ok {
		cfg.Storage.Statistics.Cache.L2.Enabled = false
	}

	// Validate configuration
	require.NoError(tb, configmap.ValidateAndNormalize(&cfg))

	return cfg
}
