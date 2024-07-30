// Package writernode provides entrypoint for the storage writer node.
// The node receives a stream of slice bytes over the network and stores them on the local disk.
package writernode

import (
	"context"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
)

func Start(ctx context.Context, d dependencies.StorageWriterScope, cfg config.Config) error {
	ctx = ctxattr.ContextWith(ctx, attribute.String("nodeId", cfg.NodeID))

	logger := d.Logger().WithComponent("storage.node.writer")
	logger.Info(ctx, `starting storage writer node`)

	return rpc.StartNetworkFileServer(d, cfg.NodeID, cfg.Hostname, cfg.Storage.Level.Local)
}
