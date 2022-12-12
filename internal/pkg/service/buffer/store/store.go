// Package store provides database operations for configuring receivers and exports
// and other backend operations.
package store

import (
	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
)

type Store struct {
	logger log.Logger
	client *etcd.Client
	clock  clock.Clock
	tracer trace.Tracer
	schema *schema.Schema
}

func New(logger log.Logger, etcdClient *etcd.Client, tracer trace.Tracer, schema *schema.Schema, clck clock.Clock) *Store {
	return &Store{
		logger: logger,
		client: etcdClient,
		clock:  clck,
		tracer: tracer,
		schema: schema,
	}
}
