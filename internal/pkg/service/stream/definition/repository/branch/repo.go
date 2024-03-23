package branch

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

const (
	MaxBranchesPerProject = 100
)

type Repository struct {
	client  etcd.KV
	plugins *plugin.Plugins
	schema  schema.Branch
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies) *Repository {
	return &Repository{
		client:  d.EtcdClient(),
		schema:  schema.ForBranch(d.EtcdSerde()),
		plugins: d.Plugins(),
	}
}
