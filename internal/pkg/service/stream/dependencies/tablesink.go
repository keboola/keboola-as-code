package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

// serviceScope implements ServiceScope interface.
type tableSinkScope struct {
	DefinitionScope
	storageRepository    *storageRepo.Repository
	statisticsL1Cache    *cache.L1
	statisticsL2Cache    *cache.L2
	statisticsRepository *statsRepo.Repository
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

func NewTableSinkScope(ctx context.Context, defScope DefinitionScope, storageConfig storage.Config) (v TableSinkScope, err error) {
	ctx, span := defScope.Telemetry().Tracer().Start(ctx, "keboola.go.buffer.dependencies.NewTableSinkScope")
	defer span.End(nil)
	return newTableSinkScope(defScope, storageConfig, model.DefaultBackoff())
}

func newTableSinkScope(defScope DefinitionScope, cfg storage.Config, backoff model.RetryBackoff) (v TableSinkScope, err error) {
	d := &tableSinkScope{}

	d.DefinitionScope = defScope

	d.statisticsRepository = statsRepo.New(d)

	d.statisticsL1Cache, err = cache.NewL1Cache(d.Logger(), d.statisticsRepository)
	if err != nil {
		return nil, err
	}

	d.statisticsL2Cache, err = cache.NewL2Cache(d.Logger(), d.Clock(), d.statisticsL1Cache, cfg.Statistics.Cache.L2)
	if err != nil {
		return nil, err
	}

	d.storageRepository = storageRepo.New(cfg.Level, d, backoff)

	return d, nil
}
