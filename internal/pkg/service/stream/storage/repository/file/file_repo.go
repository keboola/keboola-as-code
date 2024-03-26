package file

import (
	etcd "go.etcd.io/etcd/client/v3"

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

//
//// RegisterSinkType with the local storage support.
//func (r *Repository) RegisterSinkType(v definition.SinkType) {
//	r.sinkTypes[v] = true
//}
