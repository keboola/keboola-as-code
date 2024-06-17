package file

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file/schema"
	volumeRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume"
)

// Repository provides database operations with the model.File entity.
type Repository struct {
	logger     log.Logger
	client     etcd.KV
	schema     schema.File
	config     level.Config
	backoff    model.RetryBackoff
	volumes    *volumeRepo.Repository
	definition *definitionRepo.Repository
	plugins    *plugin.Plugins
}

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

func NewRepository(cfg level.Config, d dependencies, backoff model.RetryBackoff, volumes *volumeRepo.Repository) *Repository {
	r := &Repository{
		logger:     d.Logger().WithComponent("file.repository"),
		client:     d.EtcdClient(),
		schema:     schema.New(d.EtcdSerde()),
		config:     cfg,
		backoff:    backoff,
		volumes:    volumes,
		definition: d.DefinitionRepository(),
		plugins:    d.Plugins(),
	}

	r.openFileOnSinkActivation()
	r.closeFileOnSinkDeactivation()
	r.rotateFileOnSinkModification()

	return r
}

func (r *Repository) save(ctx context.Context, now time.Time, old, updated *model.File) *op.TxnOp[model.File] {
	// Call plugins
	if err := r.plugins.Executor().OnFileSave(ctx, now, old, updated); err != nil {
		return op.ErrorTxn[model.File](err)
	}

	allKey := r.schema.AllLevels().ByKey(updated.FileKey)
	inLevelKey := r.schema.InLevel(updated.State.Level()).ByKey(updated.FileKey)

	saveTxn := op.TxnWithResult(r.client, updated)
	if updated.Deleted {
		// Delete entity from All and InLevel prefixes
		saveTxn.Then(
			allKey.Delete(r.client),
			inLevelKey.Delete(r.client),
		)
	} else {
		if old == nil {
			// Entity should not exist
			saveTxn.Merge(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceAlreadyExistsError("file", updated.FileKey.String(), "sink"))
				}),
			)
		} else {
			// Entity should exist
			saveTxn.Merge(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("file", updated.FileKey.String(), "sink"))
				}),
			)
		}

		// Put entity to All and InLevel prefixes
		saveTxn.Then(
			allKey.Put(r.client, *updated),
			inLevelKey.Put(r.client, *updated),
		)

		// Remove entity from the old InLevel prefix, if needed
		if old != nil && old.State.Level() != updated.State.Level() {
			saveTxn.Then(
				r.schema.InLevel(old.State.Level()).ByKey(old.FileKey).Delete(r.client),
			)
		}
	}
	return saveTxn
}

// update reads the file, applies updateFn and save modified value.
func (r *Repository) update(k model.FileKey, now time.Time, updateFn func(model.File) (model.File, error)) *op.AtomicOp[model.File] {
	var old, updated model.File
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithResultTo(&old)
		}).
		// Update the entity
		Write(func(ctx context.Context) op.Op {
			// Update
			var err error
			updated = deepcopy.Copy(old).(model.File)
			updated, err = updateFn(updated)
			if err != nil {
				return op.ErrorOp(err)
			}

			// Save
			return r.save(ctx, now, &old, &updated)
		})
}

// loadSourceIfNil - if the source pointer is nil, a new value is allocated and later loaded,
// it will be available after the atomic operation Read phase.
func (r *Repository) loadSourceIfNil(atomicOp *op.AtomicOpCore, k key.SourceKey, source *definition.Source) *definition.Source {
	if source == nil {
		source = &definition.Source{}
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.definition.Source().Get(k).WithResultTo(source)
		})
	}
	return source
}

// loadSinkIfNil - if the sink pointer is nil, a new value is allocated and later loaded,
// it will be available after the atomic operation Read phase.
func (r *Repository) loadSinkIfNil(atomicOp *op.AtomicOpCore, k key.SinkKey, sink *definition.Sink) *definition.Sink {
	if sink == nil {
		sink = &definition.Sink{}
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.definition.Sink().Get(k).WithResultTo(sink)
		})
	}
	return sink
}
