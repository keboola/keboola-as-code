// Package configstore provides database operations for configuring receivers and exports.
package configstore

import (
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	MaxReceiversPerProject = 100
	MaxExportsPerReceiver  = 20
)

type Store struct {
	logger     log.Logger
	etcdClient *etcd.Client
	validator  validator.Validator
	tracer     trace.Tracer
	schema     *schema.Schema
}

func New(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator, tracer trace.Tracer) *Store {
	return &Store{
		logger:     logger,
		etcdClient: etcdClient,
		validator:  validator,
		tracer:     tracer,
		schema:     schema.New(validator.Validate),
	}
}
