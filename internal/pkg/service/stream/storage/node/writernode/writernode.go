// Package writernode provides entrypoint for the storage writer node.
// The node receives a stream of slice bytes over the network and stores them on the local disk.
package writernode

import (
	"context"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/registration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/volume"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	StorageRepository() *storageRepo.Repository
	StatisticsRepository() *statsRepo.Repository
}

func Start(ctx context.Context, d dependencies, cfg config.Config) error {
	clk := d.Clock()
	logger := d.Logger().WithComponent("storage.node.writer")
	logger.Info(ctx, `starting storage writer node`)

	// Open volumes
	volumes, err := volume.OpenVolumes(ctx, logger, clk, cfg.NodeID, cfg.Storage.VolumesPath, cfg.Storage.Level.Local.Writer)
	if err != nil {
		return err
	}

	// Register volumes to database
	regCfg := cfg.Storage.Level.Local.Volume.Registration
	err = registration.RegisterVolumes(regCfg, d, volumes.Collection(), d.StorageRepository().Volume().RegisterWriterVolume)
	if err != nil {
		return err
	}

	// Setup statistics collector
	syncCfg := cfg.Storage.Statistics.Collector
	collector.Start(d, volumes.Events(), syncCfg)

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		if err := volumes.Close(ctx); err != nil {
			err := errors.PrefixError(err, "`cannot close writer volumes")
			logger.Error(ctx, err.Error())
		}
	})

	return nil
}
