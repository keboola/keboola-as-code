package slice

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	fileRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/slice/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

	// Connect to the file events
	r.plugins.Collection().OnFileSave(func(ctx *plugin.Operation, old, updated *model.File) {
		// On delete
		if updated.Deleted {
			ctx.AddFrom(r.deleteAllFrom(updated.FileKey, ctx.Now()))
			return
		}

		// On create
		if old == nil {
			// Open slices
			ctx.AddFrom(r.openSlicesInFile(ctx.Now(), *updated))
			return
		}

		// On update
		if old.State != updated.State {
			switch updated.State {
			case model.FileClosing:
				// Close slices
				ctx.AddFrom(r.closeSlicesInFile(updated.FileKey, ctx.Now()))
			case model.FileImported:
				// Mark slice imported
				ctx.AddFrom(r.stateTransitionAllInFile(updated.FileKey, ctx.Now(), updated.State, model.SliceUploaded, model.SliceImported))
			default:
				// nop
			}
		}
	})

	return r
}

// ListIn lists slices in the parent, in all storage levels.
func (r *Repository) ListIn(parentKey fmt.Stringer) iterator.DefinitionT[model.Slice] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

// ListInLevel lists slices in the specified storage level.
func (r *Repository) ListInLevel(parentKey fmt.Stringer, level level.Level) iterator.DefinitionT[model.Slice] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

// ListInState lists slices in the specified state.
func (r *Repository) ListInState(parentKey fmt.Stringer, state model.SliceState) iterator.DefinitionT[model.Slice] {
	return r.
		ListInLevel(parentKey, state.Level()).
		WithFilter(func(slice model.Slice) bool {
			return slice.State == state
		})
}

// Get slice entity.
func (r *Repository) Get(k model.SliceKey) op.WithResult[model.Slice] {
	return r.schema.
		AllLevels().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("slice", k.String(), "file")
		})
}

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *Repository) IncrementRetry(now time.Time, sliceKey model.SliceKey, reason string) *op.AtomicOp[model.Slice] {
	return r.updateOne(sliceKey, now, func(slice model.Slice) (model.Slice, error) {
		slice.IncrementRetry(r.backoff, now, reason)
		return slice, nil
	})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *Repository) StateTransition(k model.SliceKey, now time.Time, from, to model.SliceState) *op.AtomicOp[model.Slice] {
	var file model.File
	return r.
		updateOne(k, now, func(slice model.Slice) (model.Slice, error) {
			return r.stateTransition(file.State, slice, now, from, to)
		}).
		ReadOp(r.files.Get(k.FileKey).WithResultTo(&file))
}

func (r *Repository) stateTransitionAllInFile(k model.FileKey, now time.Time, fileState model.FileState, from, to model.SliceState) *op.AtomicOp[[]model.Slice] {
	return r.updateAllInFile(k, now, func(slice model.Slice) (model.Slice, error) {
		return r.stateTransition(fileState, slice, now, from, to)
	})
}

func (r *Repository) stateTransition(fileState model.FileState, slice model.Slice, now time.Time, from, to model.SliceState) (model.Slice, error) {
	// Slice should be closed via one of the following ways:
	//   - Rotate/FileRepository.Rotate* methods - to create new replacement files
	//   - Close* methods - no replacement files are created.
	//   - Closing slice via StateTransition is therefore forbidden.
	if to == model.SliceClosing {
		return model.Slice{}, errors.Errorf(`unexpected transition to the state "%s", use Rotate or Close method`, model.SliceClosing)
	}

	// Validate from state
	if slice.State != from {
		return model.Slice{}, errors.Errorf(`slice "%s" is in "%s" state, expected "%s"`, slice.SliceKey, slice.State, from)
	}

	// Validate file and slice state combination
	if err := state.ValidateFileAndSliceState(fileState, to); err != nil {
		return slice, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
	}

	// Switch slice state
	return slice.WithState(now, to)
}

// update reads the file, applies updateFn and save modified value.
func (r *Repository) updateAllInFile(k model.FileKey, now time.Time, updateFn func(model.Slice) (model.Slice, error)) *op.AtomicOp[[]model.Slice] {
	var allOld, allUpdated []model.Slice
	return op.Atomic(r.client, &allUpdated).
		// Read entity for modification
		ReadOp(r.ListIn(k).WithAllTo(&allOld)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			allUpdated = nil
			saveCtx := plugin.NewSaveContext(now)
			for _, old := range allOld {
				if updated, err := r.update(saveCtx, old, updateFn); err == nil {
					allUpdated = append(allUpdated, updated)
				} else {
					return nil, err
				}
			}
			return saveCtx.Do(ctx)
		})
}

// update reads the file, applies updateFn and save modified value.
func (r *Repository) updateOne(k model.SliceKey, now time.Time, updateFn func(model.Slice) (model.Slice, error)) *op.AtomicOp[model.Slice] {
	var old, updated model.Slice
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			saveCtx := plugin.NewSaveContext(now)
			updated, err = r.update(saveCtx, old, updateFn)
			if err != nil {
				return nil, err
			}
			return saveCtx.Do(ctx)
		})
}

func (r *Repository) update(saveCtx *plugin.Operation, old model.Slice, updateFn func(model.Slice) (model.Slice, error)) (model.Slice, error) {
	// Update
	updated, err := updateFn(deepcopy.Copy(old).(model.Slice))
	if err != nil {
		return model.Slice{}, err
	}

	// Save
	r.save(saveCtx, &old, &updated)
	return updated, nil
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *model.Slice) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Do(ctx)
}

func (r *Repository) save(saveCtx *plugin.Operation, old, updated *model.Slice) {
	// Call plugins
	r.plugins.Executor().OnSliceSave(saveCtx, old, updated)

	allKey := r.schema.AllLevels().ByKey(updated.SliceKey)
	inLevelKey := r.schema.InLevel(updated.State.Level()).ByKey(updated.SliceKey)

	if updated.Deleted {
		// Delete entity from All and InLevel prefixes
		saveCtx.WriteOp(
			allKey.Delete(r.client),
			inLevelKey.Delete(r.client),
		)
	} else {
		if old == nil {
			// Entity should not exist
			saveCtx.WriteOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceAlreadyExistsError("slice", updated.SliceKey.String(), "file"))
				}),
			)
		} else {
			// Entity should exist
			saveCtx.WriteOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("slice", updated.SliceKey.String(), "file"))
				}),
			)
		}

		// Put entity to All and InLevel prefixes
		saveCtx.WriteOp(
			allKey.Put(r.client, *updated),
			inLevelKey.Put(r.client, *updated),
		)

		// Remove entity from the old InLevel prefix, if needed
		if old != nil && old.State.Level() != updated.State.Level() {
			saveCtx.WriteOp(
				r.schema.InLevel(old.State.Level()).ByKey(old.SliceKey).Delete(r.client),
			)
		}
	}
}

// Delete the slice.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *Repository) deleteAllFrom(k model.FileKey, now time.Time) *op.AtomicOp[[]model.Slice] {
	var allOld, allDeleted []model.Slice
	return op.Atomic(r.client, &allDeleted).
		ReadOp(r.ListIn(k).WithAllTo(&allOld)).
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			saveCtx := plugin.NewSaveContext(now)
			for _, old := range allOld {
				old := old

				// Mark deleted
				deleted := deepcopy.Copy(old).(model.Slice)
				deleted.Deleted = true

				// Save
				r.save(saveCtx, &old, &deleted)
				allDeleted = append(allDeleted, deleted)
			}
			return saveCtx.Do(ctx)
		})
}
