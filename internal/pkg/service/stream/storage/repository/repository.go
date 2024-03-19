package repository

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	sink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink"
	file "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file"
	slice "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/slice"
	token "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/token"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	KeboolaPublicAPI() *keboola.PublicAPI
	DefinitionRepository() *definitionRepo.Repository
	StatisticsRepository() *statsRepo.Repository
}

// Repository provides database operations with the storage entities.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	logger   log.Logger
	external *external
	sink     *sink.Repository
	file     *file.Repository
	slice    *slice.SliceRepository
	token    *token.TokenRepository
	volume   *volume.VolumeRepository
}

func New(cfg level.Config, d dependencies, backoff model.RetryBackoff) *Repository {
	r := &Repository{}
	r.logger = d.Logger()
	r.external = newExternal(cfg, d, r)
	r.sink = d.DefinitionRepository().Sink()
	r.file = file.NewRepository(cfg, d, backoff, r)
	r.slice = slice.NewRepository(d, backoff, r)
	r.token = token.NewRepository(d, r)
	r.volume = volume.NewRepository(d)
	return r
}

func (r *Repository) File() *file.Repository {
	return r.file
}

func (r *Repository) Slice() *slice.SliceRepository {
	return r.slice
}

func (r *Repository) Token() *token.TokenRepository {
	return r.token
}

func (r *Repository) Volume() *volume.VolumeRepository {
	return r.volume
}
