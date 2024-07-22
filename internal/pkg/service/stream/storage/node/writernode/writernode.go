// Package writernode provides entrypoint for the storage writer node.
// The node receives a stream of slice bytes over the network and stores them on the local disk.
package writernode

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/registration"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Volumes() *diskwriter.Volumes
	StorageRepository() *storageRepo.Repository
	StatisticsRepository() *statsRepo.Repository
}

func Start(ctx context.Context, d dependencies, cfg config.Config) error {
	ctx = ctxattr.ContextWith(ctx, attribute.String("nodeId", cfg.NodeID))

	logger := d.Logger().WithComponent("storage.node.writer")
	logger.Info(ctx, `starting storage writer node`)

	// Listen for network connections
	listener, err := transport.Listen(logger.WithComponent("server"), cfg.Storage.Level.Local.Writer.Network, cfg.NodeID)
	if err != nil {
		return err
	}
	d.Process().OnShutdown(func(ctx context.Context) {
		_ = listener.Close()
	})

	srv, err := rpc.NewNetworkFileServer(d)
	if err != nil {
		return err
	}
	d.Process().Add(func(shutdown servicectx.ShutdownFn) {
		shutdown(context.Background(), srv.Serve(listener))
	})
	d.Process().OnShutdown(func(ctx context.Context) {
		if err := listener.Close(); err != nil {
			logger.Error(ctx, err.Error())
		}
	})

	// Register volumes to database
	nodeAddress := model.RemoteAddr(fmt.Sprintf("%s:%s", cfg.Hostname, listener.Port()))
	regCfg := cfg.Storage.Level.Local.Volume.Registration
	err = registration.RegisterVolumes(regCfg, d, cfg.NodeID, nodeAddress, d.Volumes().Collection(), d.StorageRepository().Volume().RegisterWriterVolume)
	if err != nil {
		return err
	}

	return nil
}
