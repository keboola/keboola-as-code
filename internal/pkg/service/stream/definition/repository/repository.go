package repository

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
)

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	HookRegistry() *hook.Registry
	HookExecutor() *hook.Executor
}

type Repository struct {
	hooks  Hooks
	branch *BranchRepository
	source *SourceRepository
	sink   *SinkRepository
}

func New(d dependencies) *Repository {
	r := &Repository{}
	r.hooks = d.HookExecutor()
	r.branch = newBranchRepository(d, r)
	r.source = newSourceRepository(d, r)
	r.sink = newSinkRepository(d, r)
	return r
}

func (r *Repository) Branch() *BranchRepository {
	return r.branch
}

func (r *Repository) Source() *SourceRepository {
	return r.source
}

func (r *Repository) Sink() *SinkRepository {
	return r.sink
}
