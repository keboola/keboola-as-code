// Package readernode provides entrypoint for the storage reader node.
// The node uploads files from local disk to staging storage.
package readernode

import (
	"context"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func Start(ctx context.Context, d dependencies.StorageReaderScope, cfg config.Config) error {
	ctx = ctxattr.ContextWith(ctx, attribute.String("nodeId", cfg.NodeID))

	logger := d.Logger().WithComponent("storage.node.reader")
	logger.Info(ctx, `starting storage reader node`)

	return nil
}
