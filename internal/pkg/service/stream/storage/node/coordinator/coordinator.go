// Package coordinator provides the storage coordinator node.
// The node watches statistics and based on them, triggers slice upload and file import
// by modifying the state of the entity in the database.
package coordinator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/metacleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/fileimport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/filerotation"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator/slicerotation"
)

func Start(ctx context.Context, d dependencies.CoordinatorScope, cfg config.Config) error {
	logger := d.Logger().WithComponent("storage.node.coordinator")
	logger.Info(ctx, `starting storage coordinator node`)
	// Change default bridge behaviour to use Mirror as it is required for sink throttling
	err := d.KeboolaSinkBridge().MirrorJobs(d)
	if err != nil {
		return err
	}

	if err := filerotation.Start(d, cfg.Storage.Level.Target.Operator); err != nil {
		return err
	}

	if err := slicerotation.Start(d, cfg.Storage.Level.Staging.Operator); err != nil {
		return err
	}

	if err := fileimport.Start(d, cfg.Storage.Level.Target.Operator); err != nil {
		return err
	}

	if err := metacleanup.Start(d, cfg.Storage.MetadataCleanup); err != nil {
		return err
	}

	return nil
}
