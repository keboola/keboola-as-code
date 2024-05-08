package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
)

// localStorageScope implements LocalStorageScope interface.
type localStorageScope struct {
	localStorageParentScopes
	statisticsL1Cache *cache.L1
	statisticsL2Cache *cache.L2
}

type localStorageParentScopes interface {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
}

type localStorageParentScopesImpl struct {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
}

func (v *localStorageScope) StatisticsL1Cache() *cache.L1 {
	return v.statisticsL1Cache
}

func (v *localStorageScope) StatisticsL2Cache() *cache.L2 {
	return v.statisticsL2Cache
}

func NewLocalStorageScope(d localStorageParentScopes, cfg config.Config) (v LocalStorageScope, err error) {
	return newLocalStorageScope(d, cfg)
}

func newLocalStorageScope(parentScp localStorageParentScopes, cfg config.Config) (v LocalStorageScope, err error) {
	d := &localStorageScope{}

	d.localStorageParentScopes = parentScp

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
