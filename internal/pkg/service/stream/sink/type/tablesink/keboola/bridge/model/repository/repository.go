package repository

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/repository/job"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
)

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

// Repository provides database operations with the storage entities.
type Repository struct {
	job *job.Repository
}

func New(cfg level.Config, d dependencies) (*Repository, error) {
	r := &Repository{}

	r.job = job.NewRepository(d)

	return r, nil
}

func (r *Repository) Job() *job.Repository {
	return r.job
}
