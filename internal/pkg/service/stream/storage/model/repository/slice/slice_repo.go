package slice

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	fileRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository/slice/schema"
)

// Repository provides database operations with the model.Slice entity.
type Repository struct {
	logger     log.Logger
	client     *etcd.Client
	schema     schema.Slice
	backoff    model.RetryBackoff
	definition *definitionRepo.Repository
	files      *fileRepo.Repository
	plugins    *plugin.Plugins
}

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

func NewRepository(d dependencies, backoff model.RetryBackoff, files *fileRepo.Repository) *Repository {
	r := &Repository{
		logger:     d.Logger(),
		client:     d.EtcdClient(),
		schema:     schema.New(d.EtcdSerde()),
		backoff:    backoff,
		definition: d.DefinitionRepository(),
		files:      files,
		plugins:    d.Plugins(),
	}

	r.openSlicesOnFileOpen()
	r.deleteSlicesOnFileDelete()
	r.closeSliceOnFileClose()
	r.updateSlicesOnFileImport()
	r.validateSlicesOnFileStateTransition()

	return r
}

func (r *Repository) save(ctx context.Context, now time.Time, old, updated *model.Slice) *op.TxnOp[model.Slice] {
	// Call plugins
	if err := r.plugins.Executor().OnSliceSave(ctx, now, old, updated); err != nil {
		return op.ErrorTxn[model.Slice](err)
	}

	allKey := r.schema.AllLevels().ByKey(updated.SliceKey)
	inLevelKey := r.schema.InLevel(updated.State.Level()).ByKey(updated.SliceKey)

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
					r.AddErr(serviceError.NewResourceAlreadyExistsError("slice", updated.String(), "file"))
				}),
			)
		} else {
			// Entity should exist
			saveTxn.Merge(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("slice", updated.String(), "file"))
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
				r.schema.InLevel(old.State.Level()).ByKey(old.SliceKey).Delete(r.client),
			)
		}
	}

	return saveTxn
}

// update reads the slice, applies updateFn and save modified value.
func (r *Repository) update(k model.SliceKey, now time.Time, updateFn func(model.Slice) (model.Slice, error)) *op.AtomicOp[model.Slice] {
	var old, updated model.Slice
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithResultTo(&old)
		}).
		// Update the entity
		Write(func(ctx context.Context) op.Op {
			var err error
			updated = deepcopy.Copy(old).(model.Slice)
			updated, err = updateFn(updated)
			if err != nil {
				return op.ErrorOp(err)
			}

			return r.save(ctx, now, &old, &updated)
		})
}

func (r *Repository) updateAll(ctx context.Context, now time.Time, slices []model.Slice, updateFn func(model.Slice) (model.Slice, error)) *op.TxnOp[[]model.Slice] {
	var updated []model.Slice
	txn := op.TxnWithResult(r.client, &updated)
	for _, old := range slices {
		// Update
		item, err := updateFn(deepcopy.Copy(old).(model.Slice))
		if err != nil {
			return op.ErrorTxn[[]model.Slice](err)
		}
		// Save
		txn.Merge(r.save(ctx, now, &old, &item).OnSucceeded(func(r *op.TxnResult[model.Slice]) {
			updated = append(updated, r.Result())
		}))
	}
	return txn
}
