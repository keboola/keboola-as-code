package repository

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
)

type dependencies interface {
	Clock() clock.Clock
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Config() config.Config
	KeboolaPublicAPI() *keboola.PublicAPI
	DefinitionRepository() *definitionRepo.Repository
	StatisticsRepository() *statsRepo.Repository
}

// Repository provides database operations with the storage entities.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	hook   *hook
	sink   *definitionRepo.SinkRepository
	file   *FileRepository
	slice  *SliceRepository
	token  *TokenRepository
	volume *VolumeRepository
}

func New(d dependencies, backoff storage.RetryBackoff) *Repository {
	r := &Repository{}
	r.hook = newHook(d, r)
	r.sink = d.DefinitionRepository().Sink()
	r.file = newFileRepository(d, backoff, r)
	r.slice = newSliceRepository(d, backoff, r)
	r.token = newTokenRepository(d, r)
	r.volume = newVolumeRepository(d)
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

func (r *Repository) Volume() *VolumeRepository {
	return r.volume
}
