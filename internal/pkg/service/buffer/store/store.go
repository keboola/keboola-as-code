// Package store provides database operations for configuring receivers and exports
// and other backend operations.
package store

import (
	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Store struct {
	clock     clock.Clock
	logger    log.Logger
	client    *etcd.Client
	telemetry telemetry.Telemetry
	schema    *schema.Schema
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func New(d dependencies) *Store {
	return newFrom(d.Clock(), d.Logger(), d.Telemetry(), d.EtcdClient(), d.Schema())
}

func newFrom(clock clock.Clock, logger log.Logger, tel telemetry.Telemetry, etcdClient *etcd.Client, schema *schema.Schema) *Store {
	return &Store{
		clock:     clock,
		logger:    logger,
		telemetry: tel,
		client:    etcdClient,
		schema:    schema,
	}
}
