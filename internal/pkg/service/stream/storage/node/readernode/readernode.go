// Package readernode provides entrypoint for the storage reader node.
// The node uploads files from local disk to staging storage.
package readernode

import (
	"context"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/reader/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
}

func Start(ctx context.Context, d dependencies, cfg config.Config) error {
	logger := d.Logger().WithComponent("storage.node.reader")
	logger.Info(ctx, `starting storage reader node`)

	// Open volumes
	volumes, err := volume.OpenVolumes(ctx, logger, d.Clock(), cfg.NodeID, cfg.Storage.VolumesPath)
	if err != nil {
		return err
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		if err := volumes.Close(ctx); err != nil {
			err := errors.PrefixError(err, "`cannot close reader volumes")
			logger.Error(ctx, err.Error())
		}
	})

	return nil
}
