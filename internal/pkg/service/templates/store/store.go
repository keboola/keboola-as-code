// Package store provides database operations for tasks.
package store

import (
	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Store struct {
	clock  clock.Clock
	logger log.Logger
	client *etcd.Client
	tracer trace.Tracer
	schema *schema.Schema
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func New(d dependencies) *Store {
	return newFrom(d.Clock(), d.Logger(), d.Telemetry().Tracer(), d.EtcdClient(), d.Schema())
}

func newFrom(clock clock.Clock, logger log.Logger, tracer trace.Tracer, etcdClient *etcd.Client, schema *schema.Schema) *Store {
	return &Store{
		clock:  clock,
		logger: logger,
		tracer: tracer,
		client: etcdClient,
		schema: schema,
	}
}
