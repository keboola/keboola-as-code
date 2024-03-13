package repository

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
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
	HookExecutor() *hook.Executor
	DefinitionRepository() *definitionRepo.Repository
	StatisticsRepository() *statsRepo.Repository
}

// Repository provides database operations with the storage entities.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	logger   log.Logger
	hooks    Hooks
	external *external
	sink     *definitionRepo.SinkRepository
	file     *FileRepository
	slice    *SliceRepository
	token    *TokenRepository
	volume   *VolumeRepository
}

func New(cfg level.Config, d dependencies, backoff model.RetryBackoff) *Repository {
	r := &Repository{}
	r.logger = d.Logger()
	r.hooks = d.HookExecutor()
	r.external = newExternal(cfg, d, r)
	r.sink = d.DefinitionRepository().Sink()
	r.file = newFileRepository(cfg, d, backoff, r)
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
