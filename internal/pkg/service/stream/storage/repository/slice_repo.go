package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SliceRepository provides database operations with the storage.Slice entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type SliceRepository struct {
	client  etcd.KV
	schema  sliceSchema
	backoff model.RetryBackoff
	all     *Repository
}

func newSliceRepository(d dependencies, backoff model.RetryBackoff, all *Repository) *SliceRepository {
	return &SliceRepository{
		client:  d.EtcdClient(),
		schema:  newSliceSchema(d.EtcdSerde()),
		backoff: backoff,
		all:     all,
	}
}

// List slices in all storage levels.
func (r *SliceRepository) List(parentKey fmt.Stringer) iterator.DefinitionT[model.Slice] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

// ListInLevel lists slices in the specified storage level.
func (r *SliceRepository) ListInLevel(parentKey fmt.Stringer, level level.Level) iterator.DefinitionT[model.Slice] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

// ListInState lists slices in the specified state.
func (r *SliceRepository) ListInState(parentKey fmt.Stringer, state model.SliceState) iterator.DefinitionT[model.Slice] {
	return r.
		ListInLevel(parentKey, state.Level()).
		WithFilter(func(file model.Slice) bool {
			return file.State == state
		})
}

// Get slice entity.
func (r *SliceRepository) Get(k model.SliceKey) op.WithResult[model.Slice] {
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
func (r *SliceRepository) Rotate(now time.Time, fileVolumeKey model.FileVolumeKey) *op.AtomicOp[model.Slice] {
	return r.rotate(now, fileVolumeKey, true)
}

// Close closes the opened slice, if present.
//   - NO NEW SLICE is created, that's the difference with Rotate.
//   - THE OLD SLICE in the storage.SliceWriting state, IF PRESENT, is switched to the storage.SliceClosing state.
//   - This method is used to drain the volume.
func (r *SliceRepository) Close(now time.Time, fileVolumeKey model.FileVolumeKey) *op.AtomicOp[op.NoResult] {
	return op.Atomic(r.client, &op.NoResult{}).AddFrom(r.rotate(now, fileVolumeKey, false))
}

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *SliceRepository) IncrementRetry(now time.Time, sliceKey model.SliceKey, reason string) *op.AtomicOp[model.Slice] {
	return r.readAndUpdate(sliceKey, func(slice model.Slice) (model.Slice, error) {
		slice.IncrementRetry(r.backoff, now, reason)
		return slice, nil
	})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *SliceRepository) StateTransition(now time.Time, sliceKey model.SliceKey, from, to model.SliceState) *op.AtomicOp[model.Slice] {
	var file model.File
	atomicOp := r.
		readAndUpdate(sliceKey, func(slice model.Slice) (model.Slice, error) {
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
			if err := validateFileAndSliceStates(file.State, to); err != nil {
				return slice, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
			}

			// Switch slice state
			return slice.WithState(now, to)
		}).
		ReadOp(r.all.file.Get(sliceKey.FileKey).WithResultTo(&file))

	return r.all.hook.DecorateSliceStateTransition(atomicOp, sliceKey, from, to)
}

// Delete slice.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *SliceRepository) Delete(k model.SliceKey) *op.AtomicOp[op.NoResult] {
	atomicOp := op.Atomic(r.client, &op.NoResult{})

	// Delete entity from All prefix
	atomicOp.WriteOp(
		r.schema.
			AllLevels().ByKey(k).DeleteIfExists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("slice", k.String(), "file")
			}),
	)

	// Delete entity from InLevel prefixes
	for _, l := range level.AllLevels() {
		atomicOp.WriteOp(r.schema.InLevel(l).ByKey(k).Delete(r.client))
	}

	return r.all.hook.DecorateSliceDelete(atomicOp, k)
}

// rotate is a common code for rotate and close operations.
func (r *SliceRepository) rotate(now time.Time, fileVolumeKey model.FileVolumeKey, openNew bool) *op.AtomicOp[model.Slice] {
	// Init atomic operation
	var newSliceEntity model.Slice
	atomicOp := op.Atomic(r.client, &newSliceEntity)

	// Get disk space statistics to calculate pre-allocated disk space for a new slice
	var maxUsedDiskSpace map[key.SinkKey]datasize.ByteSize
	if openNew {
		provider := r.all.hook.NewUsedDiskSpaceProvider()
		atomicOp.BeforeWriteOrErr(func(ctx context.Context) (err error) {
			maxUsedDiskSpace, err = provider(ctx, []key.SinkKey{fileVolumeKey.SinkKey})
			return err
		})
	}

	// Check sink, it must exist
	atomicOp.ReadOp(r.all.sink.ExistsOrErr(fileVolumeKey.SinkKey))

	// Get file, it must exist
	var file model.File
	atomicOp.ReadOp(r.all.file.Get(fileVolumeKey.FileKey).WithResultTo(&file))

	// Read opened slices.
	// There can be a maximum of one old slice in the storage.SliceWriting state per FileVolumeKey,
	// if present, it is atomically switched to the storage.SliceClosing state.
	var openedSlices []model.Slice
	atomicOp.ReadOp(r.ListInState(fileVolumeKey, model.SliceWriting).WithAllTo(&openedSlices))

	// Close old slice, open new slice
	atomicOp.WriteOrErr(func(context.Context) (out op.Op, err error) {
		txn := op.Txn(r.client)

		// File must be in the storage.FileWriting state
		if fileState := file.State; fileState != model.FileWriting {
			return nil, serviceError.NewBadRequestError(errors.Errorf(
				`slice cannot be created: unexpected file "%s" state "%s", expected "%s"`,
				fileVolumeKey.FileKey.String(), fileState, model.FileWriting,
			))
		}

		// Open new slice, if enabled
		if openNew {
			// Create the new slice
			if newSliceEntity, err = newSlice(now, file, fileVolumeKey.VolumeID, maxUsedDiskSpace[fileVolumeKey.SinkKey]); err == nil {
				txn.Merge(r.createTxn(newSliceEntity))
			} else {
				return nil, err
			}
		}

		// Close the old slice, if any
		if count := len(openedSlices); count > 1 {
			return nil, errors.Errorf(`unexpected state, found %d opened slices in the file volume "%s"`, count, fileVolumeKey)
		} else if count == 1 {
			if oldSlice := openedSlices[0]; oldSlice.SliceKey == newSliceEntity.SliceKey {
				// Slice already exists
				return nil, serviceError.NewResourceAlreadyExistsError("slice", oldSlice.SliceKey.String(), "file")
			} else if modified, err := oldSlice.WithState(now, model.SliceClosing); err == nil {
				// Switch the old slice from the state storage.SliceWriting to the state storage.SliceCLosing
				txn.Merge(r.updateTxn(oldSlice, modified))
			} else {
				return nil, err
			}
		}

		if txn.Empty() {
			return nil, nil
		}

		return txn, nil
	})

	return atomicOp
}

func (r *SliceRepository) deleteAll(parentKey fmt.Stringer) *op.TxnOp[op.NoResult] {
	txn := op.Txn(r.client)

	// Delete entity from All prefix
	txn.Then(r.schema.AllLevels().InObject(parentKey).DeleteAll(r.client))

	// Delete entity from InLevel prefixes
	for _, l := range level.AllLevels() {
		txn.Then(r.schema.InLevel(l).InObject(parentKey).DeleteAll(r.client))
	}

	return txn
}

// createTxn saves a new entity, see also update method.
// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
// - "All" prefix is used for classic CRUD operations.
// - "InLevel" prefix is used for effective watching of the storage level.
//
//nolint:dupl // similar code is in the FileRepository
func (r *SliceRepository) createTxn(value model.Slice) *op.TxnOp[model.Slice] {
	etcdKey := r.schema.AllLevels().ByKey(value.SliceKey)
	return op.TxnWithResult(r.client, &value).
		// Entity must not exist on create
		If(etcd.Compare(etcd.ModRevision(etcdKey.Key()), "=", 0)).
		AddProcessor(func(ctx context.Context, r *op.TxnResult[model.Slice]) {
			if r.Err() == nil && !r.Succeeded() {
				r.AddErr(serviceError.NewResourceAlreadyExistsError("slice", value.SliceKey.String(), "file"))
			}
		}).
		// Put entity to All and InLevel prefixes
		Then(etcdKey.Put(r.client, value)).
		Then(r.schema.InLevel(value.State.Level()).ByKey(value.SliceKey).Put(r.client, value))
}

// updateTxn saves an existing entity, see also createTxn method.
func (r *SliceRepository) updateTxn(oldValue, newValue model.Slice) *op.TxnOp[model.Slice] {
	txn := op.TxnWithResult(r.client, &newValue)

	// Put entity to All and InLevel prefixes
	txn.
		Then(r.schema.AllLevels().ByKey(newValue.SliceKey).Put(r.client, newValue)).
		Then(r.schema.InLevel(newValue.State.Level()).ByKey(newValue.SliceKey).Put(r.client, newValue))

	// Delete entity from old level, if needed.
	if newValue.State.Level() != oldValue.State.Level() {
		txn.Then(r.schema.InLevel(oldValue.State.Level()).ByKey(oldValue.SliceKey).Delete(r.client))
	}

	return txn
}

func (r *SliceRepository) readAndUpdate(sliceKey model.SliceKey, updateFn func(slice model.Slice) (model.Slice, error)) *op.AtomicOp[model.Slice] {
	var oldValue, newValue model.Slice
	return op.Atomic(r.client, &newValue).
		// Read entity for modification
		ReadOp(r.Get(sliceKey).WithResultTo(&oldValue)).
		// Prepare the new value
		BeforeWriteOrErr(func(context.Context) (err error) {
			newValue, err = updateFn(oldValue)
			return err
		}).
		// Save the updated object
		Write(func(context.Context) op.Op {
			return r.updateTxn(oldValue, newValue)
		})
}
