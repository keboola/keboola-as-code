// Package recordstore provides database operations for temporary storage of records.
package recordstore

import (
	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	MaxImportRequestSizeInBytes = 1 * datasize.MB
	MaxMappedCSVRowSizeInBytes  = 1 * datasize.MB
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
