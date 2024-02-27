package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

// serviceScope implements ServiceScope interface.
type tableSinkScope struct {
	tableSinkParentScopes
	storageRepository    *storageRepo.Repository
	statisticsL1Cache    *cache.L1
	statisticsL2Cache    *cache.L2
	statisticsRepository *statsRepo.Repository
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

func (v *tableSinkScope) StatisticsRepository() *statsRepo.Repository {
	return v.statisticsRepository
}

func (v *tableSinkScope) StatisticsL1Cache() *cache.L1 {
	return v.statisticsL1Cache
}

func (v *tableSinkScope) StatisticsL2Cache() *cache.L2 {
	return v.statisticsL2Cache
}

func (v *tableSinkScope) StorageRepository() *storageRepo.Repository {
	return v.storageRepository
}

func NewTableSinkScope(d tableSinkParentScopes, cfg config.Config) (v TableSinkScope, err error) {
	return newTableSinkScope(d, cfg, model.DefaultBackoff())
}

func newTableSinkScope(parentScp tableSinkParentScopes, cfg config.Config, backoff model.RetryBackoff) (v TableSinkScope, err error) {
	d := &tableSinkScope{}

	d.tableSinkParentScopes = parentScp

	d.statisticsRepository = statsRepo.New(d)

	d.statisticsL1Cache, err = cache.NewL1Cache(d.Logger(), d.statisticsRepository)
	if err != nil {
		return nil, err
	}

	d.statisticsL2Cache, err = cache.NewL2Cache(d.Logger(), d.Clock(), d.statisticsL1Cache, cfg.Storage.Statistics.Cache.L2)
	if err != nil {
		return nil, err
	}

	d.storageRepository = storageRepo.New(cfg.Storage.Level, d, backoff)

	return d, nil
}
