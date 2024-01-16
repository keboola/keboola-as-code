package volume

import (
	"context"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Config() config.Config
}

type putOpFactory func(metadata storage.VolumeMetadata, id etcd.LeaseID) op.WithResult[storage.VolumeMetadata]

// RegisterVolumes in etcd with lease, so on node failure, records are automatically removed after TTL seconds.
// On session failure, volumes are registered again by the callback.
// List of the active volumes can be read by the repository.VolumeRepository.
func RegisterVolumes[V storage.Volume](d dependencies, volumes *Collection[V], putOpFactory putOpFactory) error {
	logger := d.Logger()
	client := d.EtcdClient()
	cfg := d.Config()

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		logger.InfoCtx(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		logger.InfoCtx(ctx, "shutdown done")
	})

	// Register volumes
	ttlSeconds := cfg.Sink.Table.Storage.Local.Volumes.RegistrationTTLSeconds
	errCh := etcdop.ResistantSession(ctx, wg, logger, client, ttlSeconds, func(session *concurrency.Session) error {
		txn := op.Txn(client)
		for _, vol := range volumes.All() {
			txn.Then(putOpFactory(vol.Metadata(), session.Lease()))
		}
		return txn.Do(ctx).Err()
	})

	return <-errCh
}
