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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/diskalloc"
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
	r.plugins.Collection().OnFileSave(func(ctx *plugin.SaveContext, old, updated *model.File) {
		// On file deletion, delete all slices
		if updated.Deleted {
			ctx.AddAtomicOp(r.deleteAllFrom(updated.FileKey, ctx.Now()))
			return
		}

		for _, volumeID := range updated.Assignment.Volumes {
			k := model.FileVolumeKey{FileKey: updated.FileKey, VolumeID: volumeID}
			if old == nil {
				// On file creation, create new slice
				ctx.AddAtomicOp(r.rotate(ctx.Now(), k, updated, true))
			} else if old.State != updated.State && updated.State == model.FileClosing {
				// On file closing, close the slice
				ctx.AddAtomicOp(r.rotate(ctx.Now(), k, updated, false))
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
		WithFilter(func(file model.Slice) bool {
			return file.State == state
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

// Rotate closes the opened slice, if present, and opens a new slice in the file volume.
//   - THE NEW SLICE is ALWAYS created in the state storage.SliceWriting.
//   - THE OLD SLICE in the storage.SliceWriting state, IF PRESENT, is switched to the storage.SliceClosing state.
//   - If no old slice exists, this operation effectively corresponds to the Open operation.
//   - Slices rotation is done atomically.
//   - This method is used to rotate slices when the upload conditions are met.
func (r *Repository) Rotate(now time.Time, k model.FileVolumeKey) *op.AtomicOp[model.Slice] {
	return r.rotate(now, k, nil, true)
}

// Close closes the opened slice, if present.
//   - NO NEW SLICE is created, that's the difference with Rotate.
//   - THE OLD SLICE in the storage.SliceWriting state, IF PRESENT, is switched to the storage.SliceClosing state.
//   - This method is used to drain the volume.
func (r *Repository) Close(now time.Time, k model.FileVolumeKey) *op.AtomicOp[op.NoResult] {
	return op.Atomic(r.client, &op.NoResult{}).AddFrom(r.rotate(now, k, nil, false))
}

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *Repository) IncrementRetry(now time.Time, sliceKey model.SliceKey, reason string) *op.AtomicOp[model.Slice] {
	return r.update(sliceKey, now, func(slice model.Slice) (model.Slice, error) {
		slice.IncrementRetry(r.backoff, now, reason)
		return slice, nil
	})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *Repository) StateTransition(now time.Time, k model.SliceKey, from, to model.SliceState) *op.AtomicOp[model.Slice] {
	var file model.File
	atomicOp := r.
		update(k, now, func(slice model.Slice) (model.Slice, error) {
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
			if err := state.ValidateFileAndSliceState(file.State, to); err != nil {
				return slice, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
			}

			// Switch slice state
			return slice.WithState(now, to)
		}).
		ReadOp(r.files.Get(k.FileKey).WithResultTo(&file))

	return atomicOp
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
			return saveCtx.Apply(ctx)
		})
}

// rotate is a common code for rotate and close operations.
func (r *Repository) rotate(now time.Time, k model.FileVolumeKey, file *model.File, openNewSlice bool) *op.AtomicOp[model.Slice] {
	// Init atomic operation
	var openedSlice model.Slice
	atomicOp := op.Atomic(r.client, &openedSlice)

	// Sink must exist
	atomicOp.ReadOp(r.definition.Sink().ExistsOrErr(k.SinkKey))

	// Load file
	if file == nil {
		atomicOp.ReadOp(r.files.Get(k.FileKey).WithResultTo(file))
	}

	// Load opened slices.
	// There can be a maximum of one old slice in the storage.SliceWriting state per FileVolumeKey,
	// On rotation, the opened slice is switched to the model.SliceClosing state.
	var openedSlices []model.Slice
	atomicOp.ReadOp(r.ListInState(k, model.SliceWriting).WithAllTo(&openedSlices))

	// Close old slice, open new slice
	atomicOp.WriteOrErr(func(ctx context.Context) (out op.Op, err error) {
		// There must be at most one opened file in the sink
		slicesCount := len(openedSlices)
		if slicesCount > 1 {
			return nil, errors.Errorf(`unexpected state, found %d opened slices in the file volume "%s"`, slicesCount, k)
		}

		saveCtx := plugin.NewSaveContext(now)

		// Close the old slice, if present
		if slicesCount == 1 {
			// Switch the old file from the state model.FileWriting to the state model.FileClosing
			oldSlice := openedSlices[0]
			oldUpdatedSlice, err := oldSlice.WithState(now, model.SliceClosing)
			if err != nil {
				return nil, err
			}

			// Save update old file
			r.save(saveCtx, &oldSlice, &oldUpdatedSlice)
		}

		// Open new slice, if enabled
		if openNewSlice {
			// Pass the disk allocation config to the hook in the statistics repository
			ctx = diskalloc.ContextWithConfig(ctx, file.LocalStorage.DiskAllocation)

			// File must be in the storage.FileWriting state, to open a new slice
			if fileState := file.State; fileState != model.FileWriting {
				return nil, serviceError.NewBadRequestError(errors.Errorf(
					`slice cannot be created: unexpected file "%s" state "%s", expected "%s"`,
					k.FileKey.String(), fileState, model.FileWriting,
				))
			}

			// Create slice entity
			newSlice, err := NewSlice(now, *file, k.VolumeID)
			if err != nil {
				return nil, err
			}

			// Save new file
			r.save(saveCtx, nil, &newSlice)
		}

		return saveCtx.Apply(ctx)
	})

	return atomicOp
}

// update reads the file, applies updateFn and save modified value.
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
			return r.saveOne(ctx, now, &old, &updated)
		})
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *model.Slice) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Apply(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *model.Slice) {
	// Call plugins
	r.plugins.Executor().OnSliceSave(saveCtx, old, updated)

	allKey := r.schema.AllLevels().ByKey(updated.SliceKey)
	inLevelKey := r.schema.InLevel(updated.State.Level()).ByKey(updated.SliceKey)

	if updated.Deleted {
		// Delete entity from All and InLevel prefixes
		saveCtx.AddOp(
			allKey.Delete(r.client),
			inLevelKey.Delete(r.client),
		)
	} else {
		if old == nil {
			// Entity should not exist
			saveCtx.AddOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceAlreadyExistsError("slice", updated.SliceKey.String(), "file"))
				}),
			)
		} else {
			// Entity should exist
			saveCtx.AddOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("slice", updated.SliceKey.String(), "file"))
				}),
			)
		}

		// Put entity to All and InLevel prefixes
		saveCtx.AddOp(
			allKey.Put(r.client, *updated),
			inLevelKey.Put(r.client, *updated),
		)

		// Remove entity from the old InLevel prefix, if needed
		if old != nil && old.State.Level() != updated.State.Level() {
			saveCtx.AddOp(
				r.schema.InLevel(old.State.Level()).ByKey(old.SliceKey).Delete(r.client),
			)
		}
	}
}
