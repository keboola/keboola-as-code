package repository

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/keboola/repository/token"
)

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

// Repository provides database operations with the storage entities.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	token *token.Repository
}

func New(d dependencies) *Repository {
	r := &Repository{}
	r.token = token.NewRepository(d)
	return r
}

func (r *Repository) Token() *token.Repository {
	return r.token
}
