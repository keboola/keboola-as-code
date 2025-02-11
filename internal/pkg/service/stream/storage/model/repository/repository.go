package repository

import (
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	file "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/file"
	slice "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/slice"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
	Telemetry() telemetry.Telemetry
	WatchTelemetryInterval() time.Duration
}

// Repository provides database operations with the storage entities.
type Repository struct {
	volume *volume.Repository
	file   *file.Repository
	slice  *slice.Repository
}

func New(cfg level.Config, d dependencies, backoff model.RetryBackoff) (*Repository, error) {
	r := &Repository{}

	if vr, err := volume.NewRepository(d); err == nil {
		r.volume = vr
	} else {
		return nil, err
	}

	r.file = file.NewRepository(cfg, d, backoff, r.volume)

	r.slice = slice.NewRepository(d, backoff, r.file)

	return r, nil
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
