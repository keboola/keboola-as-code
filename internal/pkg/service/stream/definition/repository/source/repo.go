package source

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

type Repository struct {
	client   etcd.KV
	schema   schema.Source
	plugins  *plugin.Plugins
	branches *branch.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies, branches *branch.Repository) *Repository {
	r := &Repository{
		client:   d.EtcdClient(),
		schema:   schema.ForSource(d.EtcdSerde()),
		plugins:  d.Plugins(),
		branches: branches,
	}

	r.deleteSourcesOnBranchDelete()
	r.undeleteSourcesOnBranchUndelete()
	return r
}
