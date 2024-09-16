// Package writernode provides entrypoint for the storage writer node.
// The node receives a stream of slice bytes over the network and stores them on the local disk.
package writernode

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode/diskcleanup"
)

func Start(ctx context.Context, d dependencies.StorageWriterScope, cfg config.Config) error {
	logger := d.Logger().WithComponent("storage.node.writer")
	logger.Info(ctx, `starting storage writer node`)

	if err := rpc.StartNetworkFileServer(d, cfg.NodeID, cfg.Hostname, cfg.Storage.Level.Local); err != nil {
		return err
	}

	if err := diskcleanup.Start(d, cfg.Storage.DiskCleanup); err != nil {
		return err
	}

	return nil
}
