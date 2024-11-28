package migrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func Run(ctx context.Context, d dependencies.MigratorScope, cfg config.Config) error {
	logger := d.Logger().WithComponent("migrator")
	logger.Info(ctx, `starting migrator`)

	return nil
}
