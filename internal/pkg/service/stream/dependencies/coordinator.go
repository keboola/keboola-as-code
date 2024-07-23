package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
)

// coordinatorScope implements CoordinatorScope interface.
type coordinatorScope struct {
	coordinatorParentScopes
	statisticsL1Cache *cache.L1
	statisticsL2Cache *cache.L2
}

type coordinatorParentScopes interface {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
}

type coordinatorParentScopesImpl struct {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
}

func (v *coordinatorScope) StatisticsL1Cache() *cache.L1 {
	return v.statisticsL1Cache
}

func (v *coordinatorScope) StatisticsL2Cache() *cache.L2 {
	return v.statisticsL2Cache
}

func NewCoordinatorScope(ctx context.Context, svcScope ServiceScope, distScp dependencies.DistributionScope, distLockScp dependencies.DistributedLockScope, cfg config.Config) (v CoordinatorScope, err error) {
	return newCoordinatorScope(ctx, coordinatorParentScopesImpl{
		ServiceScope:         svcScope,
		DistributionScope:    distScp,
		DistributedLockScope: distLockScp,
	}, cfg)
}

func NewMockedCoordinatorScope(t *testing.T, opts ...dependencies.MockedOption) (CoordinatorScope, Mocked) {
	t.Helper()
	return NewMockedCoordinatorScopeWithConfig(t, nil, opts...)
}

func NewMockedCoordinatorScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (CoordinatorScope, Mocked) {
	tb.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(
		tb,
		modifyConfig,
		append([]dependencies.MockedOption{dependencies.WithEnabledDistribution("test-node"), dependencies.WithEnabledDistributedLocks()}, opts...)...,
	)
	d, err := newCoordinatorScope(mock.TestContext(), coordinatorParentScopesImpl{
		ServiceScope:         svcScp,
		DistributionScope:    mock,
		DistributedLockScope: mock,
	}, mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func newCoordinatorScope(_ context.Context, parentScp coordinatorParentScopes, cfg config.Config) (v CoordinatorScope, err error) {
	d := &coordinatorScope{}

	d.coordinatorParentScopes = parentScp

	d.statisticsL1Cache, err = cache.NewL1Cache(d)
	if err != nil {
		return nil, err
	}

	d.statisticsL2Cache, err = cache.NewL2Cache(d, d.statisticsL1Cache, cfg.Storage.Statistics.Cache.L2)
	if err != nil {
		return nil, err
	}

	return d, nil
}
