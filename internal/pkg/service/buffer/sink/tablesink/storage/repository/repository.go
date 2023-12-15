package repository

import (
	"github.com/benbjohnson/clock"
	defRepository "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	etcd "go.etcd.io/etcd/client/v3"
)

type dependencies interface {
	Clock() clock.Clock
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

type Repository struct {
	sink  *defRepository.SinkRepository
	file  *FileRepository
	slice *SliceRepository
	token *TokenRepository
}

func New(d dependencies, definitionRepo *defRepository.Repository, cfg storage.Config) *Repository {
	return newWithBackoff(d, definitionRepo, cfg, storage.DefaultBackoff())
}

func newWithBackoff(d dependencies, definitionRepo *defRepository.Repository, cfg storage.Config, backoff storage.RetryBackoff) *Repository {
	r := &Repository{}
	r.sink = definitionRepo.Sink()
	r.file = newFileRepository(d, cfg, backoff, r)
	r.slice = newSliceRepository(d, backoff, r)
	r.token = newTokenRepository(d, r)
	return r
}

func (r *Repository) File() *FileRepository {
	return r.file
}

func (r *Repository) Slice() *SliceRepository {
	return r.slice
}

func (r *Repository) Token() *TokenRepository {
	return r.token
}
