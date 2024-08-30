// Package readernode provides entrypoint for the storage reader node.
// The node uploads files from local disk to staging storage.
package readernode

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/readernode/sliceupload"
)

func Start(ctx context.Context, d dependencies.StorageReaderScope, cfg config.Config) error {
	logger := d.Logger().WithComponent("storage.node.reader")
	logger.Info(ctx, `starting storage reader node`)
	defer logger.Info(ctx, `stoping storage reader node`)

	if err := sliceupload.Start(d, cfg.Storage.Level.Staging.Operator); err != nil {
		return err
	}

	return nil
}
