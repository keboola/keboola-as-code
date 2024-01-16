package repository

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

// VolumeRepository provides database operations with the storage.VolumeMetadata entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type VolumeRepository struct {
	client etcd.KV
	schema volumeSchema
}

func newVolumeRepository(d dependencies) *VolumeRepository {
	return &VolumeRepository{
		client: d.EtcdClient(),
		schema: newVolumeSchema(d.EtcdSerde()),
	}
}

// ListWriterVolumes lists volumes opened by writers.
func (r *VolumeRepository) ListWriterVolumes() iterator.DefinitionT[storage.VolumeMetadata] {
	return r.schema.WriterVolumes().GetAll(r.client)
}

// ListReaderVolumes lists volumes opened by readers.
func (r *VolumeRepository) ListReaderVolumes() iterator.DefinitionT[storage.VolumeMetadata] {
	return r.schema.ReaderVolumes().GetAll(r.client)
}

// RegisterWriterVolume registers an active volume on a writer node, lease ensures automatic un-registration in case of node failure.
func (r *VolumeRepository) RegisterWriterVolume(v storage.VolumeMetadata, leaseID etcd.LeaseID) op.WithResult[storage.VolumeMetadata] {
	return r.schema.WriterVolume(v.VolumeID).Put(r.client, v, etcd.WithLease(leaseID))
}

// RegisterReaderVolume registers an active volume on a reader node, lease ensures automatic un-registration in case of node failure.
func (r *VolumeRepository) RegisterReaderVolume(v storage.VolumeMetadata, leaseID etcd.LeaseID) op.WithResult[storage.VolumeMetadata] {
	return r.schema.ReaderVolume(v.VolumeID).Put(r.client, v, etcd.WithLease(leaseID))
}
