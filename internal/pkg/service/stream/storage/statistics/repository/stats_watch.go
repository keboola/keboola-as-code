package repository

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) GetAllAndWatch(ctx context.Context) *etcdop.RestartableWatchStreamT[statistics.Value] {
	return r.schema.GetAllAndWatch(ctx, r.client)
}
