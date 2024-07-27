// Package coordinator provides the storage coordinator node.
// The node watches statistics and based on them, triggers slice upload and file import
// by modifying the state of the entity in the database.
package coordinator

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

func Start(ctx context.Context, d dependencies.CoordinatorScope, cfg config.Config) error {
	ctx = ctxattr.ContextWith(ctx, attribute.String("nodeId", cfg.NodeID))

	logger := d.Logger().WithComponent("storage.node.coordinator")
	logger.Info(ctx, `starting storage coordinator node`)

	return nil
}
