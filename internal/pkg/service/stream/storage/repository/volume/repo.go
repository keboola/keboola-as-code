package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume/schema"
	etcd "go.etcd.io/etcd/client/v3"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

// Repository provides database operations with the storage.Metadata entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	client etcd.KV
	schema schema.Volume
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewRepository(d dependencies) *Repository {
	return &Repository{
		client: d.EtcdClient(),
		schema: schema.New(d.EtcdSerde()),
	}
}

// AssignVolumes assigns volumes to a new file.
func (r *Repository) AssignVolumes(allVolumes []volume.Metadata, cfg assignment.Config, fileOpenedAt time.Time) assignment.Assignment {
	return assignment.VolumesFor(allVolumes, cfg, fileOpenedAt.UnixNano())
}

// ListWriterVolumes lists volumes opened by writers.
func (r *Repository) ListWriterVolumes() iterator.DefinitionT[volume.Metadata] {
	return r.schema.WriterVolumes().GetAll(r.client)
}

// ListReaderVolumes lists volumes opened by readers.
func (r *Repository) ListReaderVolumes() iterator.DefinitionT[volume.Metadata] {
	return r.schema.ReaderVolumes().GetAll(r.client)
}

// RegisterWriterVolume registers an active volume on a writer node, lease ensures automatic un-registration in case of node failure.
func (r *Repository) RegisterWriterVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata] {
	return r.schema.WriterVolume(v.ID).Put(r.client, v, etcd.WithLease(leaseID))
}

// RegisterReaderVolume registers an active volume on a reader node, lease ensures automatic un-registration in case of node failure.
func (r *Repository) RegisterReaderVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata] {
	return r.schema.ReaderVolume(v.ID).Put(r.client, v, etcd.WithLease(leaseID))
}
