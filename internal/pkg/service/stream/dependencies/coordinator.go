package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
)

// coordinatorScope implements CoordinatorScope interface.
type coordinatorScope struct {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
	statisticsL1Cache *cache.L1
	statisticsL2Cache *cache.L2
}

func (v *coordinatorScope) StatisticsL1Cache() *cache.L1 {
	return v.statisticsL1Cache
}

func (v *coordinatorScope) StatisticsL2Cache() *cache.L2 {
	return v.statisticsL2Cache
}

func NewCoordinatorScope(ctx context.Context, svcScope ServiceScope, distScp dependencies.DistributionScope, distLockScp dependencies.DistributedLockScope, cfg config.Config) (v CoordinatorScope, err error) {
	return newCoordinatorScope(ctx, svcScope, distScp, distLockScp, cfg)
}

func NewMockedCoordinatorScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (CoordinatorScope, Mocked) {
	tb.Helper()
	return NewMockedCoordinatorScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedCoordinatorScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (CoordinatorScope, Mocked) {
	tb.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(tb, ctx, modifyConfig, opts...)

	distScp := dependencies.NewDistributionScope(mock.TestConfig().NodeID, mock.TestConfig().Distribution, svcScp)
	distLockScp, err := dependencies.NewDistributedLockScope(ctx, distlock.NewConfig(), svcScp)
	require.NoError(tb, err)

	d, err := newCoordinatorScope(ctx, svcScp, distScp, distLockScp, mock.TestConfig())
	require.NoError(tb, err)

	return d, mock
}

func newCoordinatorScope(_ context.Context, svcScp ServiceScope, distScp dependencies.DistributionScope, distLockScp dependencies.DistributedLockScope, cfg config.Config) (v CoordinatorScope, err error) {
	d := &coordinatorScope{}

	d.ServiceScope = svcScp

	d.DistributionScope = distScp

	d.DistributedLockScope = distLockScp

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
