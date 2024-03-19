package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume/schema"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

// VolumeRepository provides database operations with the storage.Metadata entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type VolumeRepository struct {
	client etcd.KV
	schema schema.Volume
}

func NewRepository(d repository.dependencies) *VolumeRepository {
	return &VolumeRepository{
		client: d.EtcdClient(),
		schema: schema.ForVolume(d.EtcdSerde()),
	}
}

// ListWriterVolumes lists volumes opened by writers.
func (r *VolumeRepository) ListWriterVolumes() iterator.DefinitionT[volume.Metadata] {
	return r.schema.WriterVolumes().GetAll(r.client)
}

// ListReaderVolumes lists volumes opened by readers.
func (r *VolumeRepository) ListReaderVolumes() iterator.DefinitionT[volume.Metadata] {
	return r.schema.ReaderVolumes().GetAll(r.client)
}

// RegisterWriterVolume registers an active volume on a writer node, lease ensures automatic un-registration in case of node failure.
func (r *VolumeRepository) RegisterWriterVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata] {
	return r.schema.WriterVolume(v.ID).Put(r.client, v, etcd.WithLease(leaseID))
}

// RegisterReaderVolume registers an active volume on a reader node, lease ensures automatic un-registration in case of node failure.
func (r *VolumeRepository) RegisterReaderVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata] {
	return r.schema.ReaderVolume(v.ID).Put(r.client, v, etcd.WithLease(leaseID))
}
