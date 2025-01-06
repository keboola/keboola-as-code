// Package store provides database operations for tasks.
package store

import (
	"github.com/jonboulle/clockwork"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Store struct {
	clock  clockwork.Clock
	logger log.Logger
	client *etcd.Client
	tracer telemetry.Tracer
	schema *schema.Schema
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func New(d dependencies) *Store {
	return newFrom(d.Clock(), d.Logger(), d.Telemetry().Tracer(), d.EtcdClient(), d.Schema())
}

func newFrom(clock clockwork.Clock, logger log.Logger, tracer telemetry.Tracer, etcdClient *etcd.Client, schema *schema.Schema) *Store {
	return &Store{
		clock:  clock,
		logger: logger,
		tracer: tracer,
		client: etcdClient,
		schema: schema,
	}
}
