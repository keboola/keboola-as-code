package slice

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) GetAllInLevelAndWatch(ctx context.Context, level storage.Level, opts ...etcd.OpOption) *etcdop.RestartableWatchStreamT[storage.Slice] {
	return r.schema.InLevel(level).GetAllAndWatch(ctx, r.client, opts...)
}
