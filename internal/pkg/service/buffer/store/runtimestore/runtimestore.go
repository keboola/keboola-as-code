// Package runtimestore provides database operations for statistics and API/Worker synchronization.
package runtimestore

import (
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Store struct {
	logger     log.Logger
	etcdClient *etcd.Client
	validator  validator.Validator
	tracer     trace.Tracer
}

func New(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator, tracer trace.Tracer) *Store {
	return &Store{logger, etcdClient, validator, tracer}
}
