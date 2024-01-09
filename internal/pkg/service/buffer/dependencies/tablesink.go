package dependencies

import (
	"context"

	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/repository"
)

// serviceScope implements ServiceScope interface.
type tableSinkScope struct {
	DefinitionScope
	storageRepository    *storageRepo.Repository
	statisticsRepository *statsRepo.Repository
}

func (v *tableSinkScope) StatisticsRepository() *statsRepo.Repository {
	return v.statisticsRepository
}

func (v *tableSinkScope) StorageRepository() *storageRepo.Repository {
	return v.storageRepository
}

func NewTableSinkScope(ctx context.Context, defScope DefinitionScope) (v TableSinkScope) {
	ctx, span := defScope.Telemetry().Tracer().Start(ctx, "keboola.go.buffer.dependencies.NewTableSinkScope")
	defer span.End(nil)
	return newTableSinkScope(defScope, storage.DefaultBackoff())
}

func newTableSinkScope(defScope DefinitionScope, backoff storage.RetryBackoff) (v TableSinkScope) {
	d := &tableSinkScope{}

	d.DefinitionScope = defScope

	d.statisticsRepository = statsRepo.New(d)

	d.storageRepository = storageRepo.New(d, backoff)

	return d
}
