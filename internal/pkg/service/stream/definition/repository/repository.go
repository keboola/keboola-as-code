package repository

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	branch "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	sink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink"
	source "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

type Repository struct {
	branch *branch.Repository
	source *source.Repository
	sink   *sink.Repository
}

func New(d dependencies) *Repository {
	r := &Repository{}
	r.branch = branch.NewRepository(d)
	r.source = source.NewRepository(d, r.branch)
	r.sink = sink.NewRepository(d, r.source)
	return r
}

func (r *Repository) Branch() *branch.Repository {
	return r.branch
}

func (r *Repository) Source() *source.Repository {
	return r.source
}

func (r *Repository) Sink() *sink.Repository {
	return r.sink
}
