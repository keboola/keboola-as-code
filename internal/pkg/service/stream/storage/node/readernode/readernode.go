// Package readernode provides entrypoint for the storage reader node.
// The node uploads files from local disk to staging storage.
package readernode

import (
	"context"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Volumes() *diskreader.Volumes
}

func Start(ctx context.Context, d dependencies, cfg config.Config) error {
	ctx = ctxattr.ContextWith(ctx, attribute.String("nodeId", cfg.NodeID))

	logger := d.Logger().WithComponent("storage.node.reader")
	logger.Info(ctx, `starting storage reader node`)

	return nil
}
