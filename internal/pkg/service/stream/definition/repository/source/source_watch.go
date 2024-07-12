package source

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func (r *Repository) GetAllAndWatch(ctx context.Context, opts ...etcd.OpOption) *etcdop.RestartableWatchStreamT[definition.Source] {
	return r.schema.Active().GetAllAndWatch(ctx, r.client, opts...)
}
