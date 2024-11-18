package job

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
)

func (r *Repository) GetAllAndWatch(ctx context.Context, opts ...etcd.OpOption) *etcdop.RestartableWatchStreamT[model.Job] {
	return r.schema.GetAllAndWatch(ctx, r.client, opts...)
}
