package file

import (
	"fmt"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file/schema"
	volumeRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume"
)

// Repository provides database operations with the model.File entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	client     etcd.KV
	schema     schema.File
	config     level.Config
	backoff    model.RetryBackoff
	volumes    *volumeRepo.Repository
	definition *definitionRepo.Repository
	plugins    *plugin.Plugins
	// sinkTypes defines which sinks use local storage
	sinkTypes map[definition.SinkType]bool
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

func NewRepository(cfg level.Config, d dependencies, backoff model.RetryBackoff, volumes *volumeRepo.Repository) *Repository {
	r := &Repository{
		client:     d.EtcdClient(),
		schema:     schema.ForFile(d.EtcdSerde()),
		config:     cfg,
		backoff:    backoff,
		volumes:    volumes,
		definition: d.DefinitionRepository(),
		plugins:    d.Plugins(),
		sinkTypes:  make(map[definition.SinkType]bool),
	}

	r.openFileOnSinkActivation()
	r.closeFileOnSinkDeactivation()
	r.rotateFileOnSinkModification()

	// Connect to the sink events
	r.plugins.Collection().OnSinkSave(func(ctx *plugin.SaveContext, old, updated *definition.Sink) {
		// Skip unsupported sink type
		if !r.sinkTypes[updated.Type] {
			return
		}

		createdOrModified := !updated.Deleted && !updated.Disabled
		deleted := updated.Deleted && updated.DeletedAt.Time().Equal(ctx.Now())
		disabled := updated.Disabled && updated.DisabledAt.Time().Equal(ctx.Now())
		deactivated := deleted || disabled
		if createdOrModified {
			// Rotate file on the sink creation/modification
			ctx.AddFrom(r.Rotate(updated.SinkKey, ctx.Now()))
		} else if deactivated {
			// Close file on the sink deactivation
			ctx.AddFrom(r.Close(updated.SinkKey, ctx.Now()))
		}
	})

	return r
}

// RegisterSinkType with the local storage support.
func (r *Repository) RegisterSinkType(v definition.SinkType) {
	r.sinkTypes[v] = true
}

// ListAll files in all storage levels.
func (r *Repository) ListAll() iterator.DefinitionT[model.File] {
	return r.schema.AllLevels().GetAll(r.client)
}

// ListIn files in all storage levels, in the parent.
func (r *Repository) ListIn(parentKey fmt.Stringer) iterator.DefinitionT[model.File] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

// ListInLevel lists files in the specified storage level.
func (r *Repository) ListInLevel(parentKey fmt.Stringer, level level.Level) iterator.DefinitionT[model.File] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

// ListInState lists files in the specified state.
func (r *Repository) ListInState(parentKey fmt.Stringer, state model.FileState) iterator.DefinitionT[model.File] {
	return r.
		ListInLevel(parentKey, state.Level()).
		WithFilter(func(file model.File) bool {
			return file.State == state
		})
}

// Get file entity.
func (r *Repository) Get(fileKey model.FileKey) op.WithResult[model.File] {
	return r.schema.AllLevels().ByKey(fileKey).Get(r.client).WithEmptyResultAsError(func() error {
		return serviceError.NewResourceNotFoundError("file", fileKey.String(), "sink")
	})
}
