package slice

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	fileRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/slice/schema"
)

// Repository provides database operations with the storage.Slice entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	client     etcd.KV
	schema     schema.Slice
	backoff    model.RetryBackoff
	definition *definitionRepo.Repository
	files      *fileRepo.Repository
	plugins    *plugin.Plugins
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

func NewRepository(d dependencies, backoff model.RetryBackoff, files *fileRepo.Repository) *Repository {
	r := &Repository{
		client:     d.EtcdClient(),
		schema:     schema.New(d.EtcdSerde()),
		backoff:    backoff,
		definition: d.DefinitionRepository(),
		files:      files,
		plugins:    d.Plugins(),
	}

	r.openSlicesOnFileCreation()
	r.stateTransitionWithFile()

	return r
}

// update reads the slice, applies updateFn and save modified value.
func (r *Repository) update(k model.SliceKey, now time.Time, updateFn func(model.Slice) (model.Slice, error)) *op.AtomicOp[model.Slice] {
	var old, updated model.Slice
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Update
			updated = deepcopy.Copy(old).(model.Slice)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			return r.save(ctx, now, &old, &updated), nil
		})
}

func (r *Repository) save(ctx context.Context, now time.Time, old, updated *model.Slice) *op.TxnOp[model.Slice] {
	// Call plugins
	r.plugins.Executor().OnSliceSave(ctx, now, old, updated)

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
					r.AddErr(serviceError.NewResourceAlreadyExistsError("slice", updated.SliceKey.String(), "file"))
				}),
			)
		} else {
			// Entity should exist
			saveTxn.Merge(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("slice", updated.SliceKey.String(), "file"))
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
