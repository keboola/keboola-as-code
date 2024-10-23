package job

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/job/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink"
)

type Repository struct {
	client *etcd.Client
	schema schema.Job
	sinks  *sink.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewRepository(d dependencies, sinks *sink.Repository) *Repository {
	r := &Repository{
		client: d.EtcdClient(),
		schema: schema.New(d.EtcdSerde()),
		sinks:  sinks,
	}

	return r
}
