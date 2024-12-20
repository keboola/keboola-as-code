package migrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func Run(ctx context.Context, d dependencies.MigratorScope, _ config.Config) error {
	logger := d.Logger().WithComponent("migrator")
	logger.Info(ctx, `starting migrator`)

	err := d.KeboolaSinkBridge().MigrateTokens(ctx)
	if err != nil {
		return err
	}

	return nil
}
