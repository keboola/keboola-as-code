package sink

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

type Repository struct {
	client  etcd.KV
	schema  schema.Sink
	plugins *plugin.Plugins
	sources *source.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies, sources *source.Repository) *Repository {
	r := &Repository{
		client:  d.EtcdClient(),
		schema:  schema.ForSink(d.EtcdSerde()),
		plugins: d.Plugins(),
		sources: sources,
	}

	r.deleteSinksOnSourceDelete()
	r.undeleteSinksOnSourceUndelete()
	return r
}
