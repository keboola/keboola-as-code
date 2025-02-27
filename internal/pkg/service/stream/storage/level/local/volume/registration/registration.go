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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
func RegisterVolumes[V volume.Volume](cfg Config, d dependencies, nodeID string, nodeAddress volume.RemoteAddr, volumes *volume.Collection[V], putOpFactory putOpFactory) error {
	logger := d.Logger().WithComponent("volumes.registry")
	client := d.EtcdClient()

	// Graceful shutdown
	ctx, cancel := context.WithCancelCause(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "stopping volumes registration")
		cancel(errors.New("shutting down: volumes registration"))
		wg.Wait()
		logger.Info(ctx, "stopped volumes registration")
	})

	// Register volumes
	_, errCh := etcdop.
		NewSessionBuilder().
		WithTTLSeconds(cfg.TTLSeconds).
		WithOnSession(func(session *concurrency.Session) error {
			txn := op.Txn(client)
			all := volumes.All()
			for _, vol := range all {
				metadata := vol.Metadata()
				metadata.NodeID = nodeID
				metadata.NodeAddress = nodeAddress
				txn.Merge(putOpFactory(metadata, session.Lease()))
			}
			if err := txn.Do(ctx).Err(); err != nil {
				err := errors.PrefixError(err, `cannot register volumes to database`)
				logger.Error(ctx, err.Error())
				return err
			}

			logger.Infof(ctx, `registered "%d" volumes`, len(all))
			return nil
		}).
		Start(ctx, wg, logger, client)
	return <-errCh
}
