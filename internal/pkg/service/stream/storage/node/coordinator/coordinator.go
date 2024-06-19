// Package coordinator provides the storage coordinator node.
// The node watches statistics and based on them, triggers slice upload and file import
// by modifying the state of the entity in the database.
package coordinator

import (
	"context"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
}

func Start(ctx context.Context, d dependencies, cfg config.Config) error {
	logger := d.Logger().WithComponent("storage.node.coordinator")
	logger.Info(ctx, `starting storage coordinator node`)
	return nil
}
