package registration

import (
	"context"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
}

type putOpFactory func(metadata volume.Metadata, id etcd.LeaseID) op.WithResult[volume.Metadata]

// RegisterVolumes in etcd with lease, so on node failure, records are automatically removed after TTL seconds.
// On session failure, volumes are registered again by the callback.
// List of the active volumes can be read by the repository.VolumeRepository.
func RegisterVolumes[V volume.Volume](cfg Config, d dependencies, volumes *volume.Collection[V], putOpFactory putOpFactory) error {
	logger := d.Logger().WithComponent("volumes.registry")
	client := d.EtcdClient()

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "stopping volumes registration")
		cancel()
		wg.Wait()
		logger.Info(ctx, "stopped volumes registration")
	})

	// Register volumes
	_, errCh := etcdop.
		NewSessionBuilder().
		WithTTLSeconds(cfg.TTLSeconds).
		WithOnSession(func(session *concurrency.Session) error {
			txn := op.Txn(client)
			for _, vol := range volumes.All() {
				txn.Merge(putOpFactory(vol.Metadata(), session.Lease()))
			}
			return txn.Do(ctx).Err()
		}).
		Start(ctx, wg, logger, client)
	return <-errCh
}
