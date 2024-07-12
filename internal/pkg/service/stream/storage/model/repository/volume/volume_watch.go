package volume

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

func (r *Repository) GetAllWriterVolumesAndWatch(ctx context.Context, opts ...etcd.OpOption) *etcdop.RestartableWatchStreamT[volume.Metadata] {
	return r.schema.WriterVolumes().GetAllAndWatch(ctx, r.client, opts...)
}
