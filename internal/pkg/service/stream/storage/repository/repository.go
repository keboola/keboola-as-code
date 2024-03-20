package repository

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	file "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file"
	slice "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/slice"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume"
)

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

// Repository provides database operations with the storage entities.
type Repository struct {
	volume *volume.Repository
	file   *file.Repository
	slice  *slice.Repository
}

func New(cfg level.Config, d dependencies, backoff model.RetryBackoff) *Repository {
	r := &Repository{}
	r.volume = volume.NewRepository(d)
	r.file = file.NewRepository(cfg, d, backoff, r.volume)
	r.slice = slice.NewRepository(d, backoff, r.file)
	return r
}

func (r *Repository) Volume() *volume.Repository {
	return r.volume
}

func (r *Repository) File() *file.Repository {
	return r.file
}

func (r *Repository) Slice() *slice.Repository {
	return r.slice
}
