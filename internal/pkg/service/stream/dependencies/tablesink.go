package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
)

// serviceScope implements ServiceScope interface.
type tableSinkScope struct {
	tableSinkParentScopes
	statisticsL1Cache *cache.L1
	statisticsL2Cache *cache.L2
}

type tableSinkParentScopes interface {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
}

type tableSinkParentScopesImpl struct {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
}

func (v *tableSinkScope) StatisticsL1Cache() *cache.L1 {
	return v.statisticsL1Cache
}

func (v *tableSinkScope) StatisticsL2Cache() *cache.L2 {
	return v.statisticsL2Cache
}

func NewTableSinkScope(d tableSinkParentScopes, cfg config.Config) (v TableSinkScope, err error) {
	return newTableSinkScope(d, cfg)
}

func newTableSinkScope(parentScp tableSinkParentScopes, cfg config.Config) (v TableSinkScope, err error) {
	d := &tableSinkScope{}

	d.tableSinkParentScopes = parentScp

	d.statisticsL1Cache, err = cache.NewL1Cache(d.Logger(), d.StatisticsRepository())
	if err != nil {
		return nil, err
	}

	d.statisticsL2Cache, err = cache.NewL2Cache(d.Logger(), d.Clock(), d.statisticsL1Cache, cfg.Storage.Statistics.Cache.L2)
	if err != nil {
		return nil, err
	}

	return d, nil
}
