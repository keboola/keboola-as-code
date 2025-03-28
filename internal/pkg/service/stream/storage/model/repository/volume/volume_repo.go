package volume

import (
	"context"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/volume/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Repository provides database operations with the storage.Metadata entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	logger  log.Logger
	process *servicectx.Process
	client  *etcd.Client
	schema  schema.Volume
	volumes *etcdop.MirrorTree[volume.Metadata, volume.Metadata]
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

func NewRepository(d dependencies) (*Repository, error) {
	r := &Repository{
		logger:  d.Logger().WithComponent("volume.repository"),
		process: d.Process(),
		client:  d.EtcdClient(),
		schema:  schema.New(d.EtcdSerde()),
	}

	if err := r.watchVolumes(d); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Repository) watchVolumes(d dependencies) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	wg := &sync.WaitGroup{}
	r.process.OnShutdown(func(ctx context.Context) {
		r.logger.Info(ctx, "closing volumes stream")
		cancel(errors.New("shutting down: volumes stream"))
		wg.Wait()
		r.logger.Info(ctx, "closed volumes stream")
	})

	r.volumes = etcdop.SetupFullMirrorTree(r.schema.WriterVolumes().GetAllAndWatch(ctx, r.client)).BuildMirror()
	return <-r.volumes.StartMirroring(ctx, wg, r.logger, d.Telemetry(), d.WatchTelemetryInterval())
}

// AssignVolumes assigns volumes to a new file.
func (r *Repository) AssignVolumes(cfg assignment.Config, fileOpenedAt time.Time) assignment.Assignment {
	return assignment.VolumesFor(r.volumes.All(), cfg, fileOpenedAt.UnixNano())
}

// ListWriterVolumes lists volumes opened by writers.
func (r *Repository) ListWriterVolumes(opts ...iterator.Option) iterator.DefinitionT[volume.Metadata] {
	return r.schema.WriterVolumes().GetAll(r.client, opts...)
}

// ListReaderVolumes lists volumes opened by readers.
func (r *Repository) ListReaderVolumes(opts ...iterator.Option) iterator.DefinitionT[volume.Metadata] {
	return r.schema.ReaderVolumes().GetAll(r.client, opts...)
}

// RegisterWriterVolume registers an active volume on a writer node, lease ensures automatic un-registration in case of node failure.
func (r *Repository) RegisterWriterVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata] {
	return r.schema.WriterVolume(v.ID).Put(r.client, v, etcd.WithLease(leaseID))
}

// RegisterReaderVolume registers an active volume on a reader node, lease ensures automatic un-registration in case of node failure.
func (r *Repository) RegisterReaderVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata] {
	return r.schema.ReaderVolume(v.ID).Put(r.client, v, etcd.WithLease(leaseID))
}
