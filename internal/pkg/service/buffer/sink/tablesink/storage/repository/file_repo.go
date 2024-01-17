package repository

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// FileRepository provides database operations with the storage.File entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type FileRepository struct {
	client  etcd.KV
	schema  fileSchema
	config  storage.Config
	backoff storage.RetryBackoff
	all     *Repository
}

func newFileRepository(d dependencies, backoff storage.RetryBackoff, all *Repository) *FileRepository {
	return &FileRepository{
		client:  d.EtcdClient(),
		schema:  newFileSchema(d.EtcdSerde()),
		config:  d.Config().Sink.Table.Storage,
		backoff: backoff,
		all:     all,
	}
}

// List files in all storage levels.
func (r *FileRepository) List(parentKey fmt.Stringer) iterator.DefinitionT[storage.File] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

// ListInLevel lists files in the specified storage level.
func (r *FileRepository) ListInLevel(parentKey fmt.Stringer, level storage.Level) iterator.DefinitionT[storage.File] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

// ListInState lists files in the specified state.
func (r *FileRepository) ListInState(parentKey fmt.Stringer, state storage.FileState) iterator.DefinitionT[storage.File] {
	return r.
		ListInLevel(parentKey, state.Level()).
		WithFilter(func(file storage.File) bool {
			return file.State == state
		})
}

// Get file entity.
func (r *FileRepository) Get(fileKey storage.FileKey) op.WithResult[storage.File] {
	return r.schema.AllLevels().ByKey(fileKey).Get(r.client).WithEmptyResultAsError(func() error {
		return serviceError.NewResourceNotFoundError("file", fileKey.String(), "sink")
	})
}

// Rotate closes the opened file, if present, and opens a new file in the table sink.
//   - The old file, if present, is switched from the storage.FileWriting state to the storage.FileClosing state.
//   - New file in the storage.FileWriting is created.
//   - AllKVs file slices in the storage.SliceWriting state are switched to the storage.SliceClosing state.
//   - Opening new slices in the file, on different volumes, is not the task of this method.
//   - Files rotation is done atomically.
//   - This method is used to rotate files when the import conditions are met.
func (r *FileRepository) Rotate(rb rollback.Builder, now time.Time, sinkKey key.SinkKey) *op.AtomicOp[storage.File] {
	var sink definition.Sink
	return r.
		rotate(rb, now, sinkKey, &sink, true).
		ReadOp(r.all.sink.Get(sinkKey).WithResultTo(&sink))
}

// RotateOnSinkMod it similar to Rotate method, but the Sink value is provided directly, not read from the database.
//   - The method should be used only on a Sink create/update, to create the first file with the new Sink mapping.
//   - Otherwise, use the Rotate method.
func (r *FileRepository) RotateOnSinkMod(rb rollback.Builder, now time.Time, sink definition.Sink) *op.AtomicOp[storage.File] {
	return r.rotate(rb, now, sink.SinkKey, &sink, true)
}

// RotateAllIn is same as Rotate method, but it is applied for each table sink within the parentKey.
// - This method is used on Sink/Source undelete or enable operation.
func (r *FileRepository) RotateAllIn(rb rollback.Builder, now time.Time, parentKey fmt.Stringer) *op.AtomicOp[[]storage.File] {
	return r.rotateAllIn(rb, now, parentKey, nil, true)
}

// CloseAllIn closes opened file in each table sink within the parentKey.
// - NO NEW FILE is created, so the sink stops accepting new writes, that's the difference with RotateAllIn.
// - THE OLD FILE in the storage.FileWriting state, IF PRESENT, is switched to the storage.FileClosing state.
// - Files closing is done atomically.
// - This method is used on Sink/Source soft-delete or disable operation.
func (r *FileRepository) CloseAllIn(now time.Time, parentKey fmt.Stringer) *op.AtomicOp[op.NoResult] {
	return op.Atomic(r.client, &op.NoResult{}).AddFrom(r.rotateAllIn(nil, now, parentKey, nil, false))
}

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *FileRepository) IncrementRetry(now time.Time, k storage.FileKey, reason string) *op.AtomicOp[storage.File] {
	return r.
		readAndUpdate(k, func(slice storage.File) (storage.File, error) {
			slice.IncrementRetry(r.backoff, now, reason)
			return slice, nil
		})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *FileRepository) StateTransition(now time.Time, fileKey storage.FileKey, from, to storage.FileState) *op.AtomicOp[storage.File] {
	var fileSlices []storage.Slice
	atomicOp := r.
		// Modify the file
		readAndUpdate(fileKey, func(file storage.File) (storage.File, error) {
			// File should be closed via one of the following ways:
			//   - Rotate* methods - to create new replacement files
			//   - Close* methods - no replacement files are created.
			//   - Closing file via StateTransition is therefore forbidden.
			if to == storage.FileClosing {
				return storage.File{}, errors.Errorf(`unexpected file transition to the state "%s", use Rotate* or Close* methods`, storage.FileClosing)
			}

			// Validate from state
			if file.State != from {
				return storage.File{}, errors.Errorf(`file "%s" is in "%s" state, expected "%s"`, file.FileKey, file.State, from)
			}

			// Switch file state
			return file.WithState(now, to)
		}).
		// Read slices for modification
		ReadOp(r.all.slice.List(fileKey).WithAllTo(&fileSlices)).
		// Modify slices states, if needed
		WriteOrErr(func(context.Context) (out op.Op, err error) {
			txn := op.Txn(r.client)
			errs := errors.NewMultiError()
			for _, slice := range fileSlices {
				oldSliceState := slice
				if to == storage.FileClosing && slice.State == storage.SliceWriting {
					// Switch slice state on FileClosing
					if slice, err = slice.WithState(now, storage.SliceClosing); err != nil {
						errs.Append(err)
						continue
					}
				} else if to == storage.FileImported && slice.State == storage.SliceUploaded {
					// Switch slice state on FileImported
					if slice, err = slice.WithState(now, storage.SliceImported); err != nil {
						errs.Append(err)
						continue
					}
				}

				// Validate file and slice state combination
				if err = validateFileAndSliceStates(to, slice.State); err != nil {
					return nil, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
				}

				// Save modified value, if there is a difference
				if !reflect.DeepEqual(oldSliceState, slice) {
					txn.Merge(r.all.slice.updateTxn(oldSliceState, slice))
				}
			}

			if err = errs.ErrorOrNil(); err != nil {
				return nil, err
			}

			if !txn.Empty() {
				return txn, nil
			}

			return nil, nil
		})

	return r.all.hook.DecorateFileStateTransition(atomicOp, now, fileKey, from, to)
}

// Delete file a file slices.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *FileRepository) Delete(k storage.FileKey) *op.TxnOp[op.NoResult] {
	txn := op.Txn(r.client)

	// Delete entity from All prefix
	txn.Merge(
		r.schema.
			AllLevels().ByKey(k).DeleteIfExists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("file", k.String(), "sink")
			}),
	)

	// Delete entity from InLevel prefixes
	for _, l := range storage.AllLevels() {
		txn.Then(r.schema.InLevel(l).ByKey(k).Delete(r.client))
	}

	// Delete all slices
	txn.Merge(r.all.slice.deleteAll(k))

	return txn
}

// rotate one file, it is a special case of the rotateAllIn.
func (r *FileRepository) rotate(rb rollback.Builder, now time.Time, sinkKey key.SinkKey, sink *definition.Sink, openNewFile bool) *op.AtomicOp[storage.File] {
	var file storage.File
	var sinks []definition.Sink
	return op.Atomic(r.client, &file).
		BeforeWrite(func(context.Context) {
			// Create sinks slice before write, after the read phase, because the sink value may not be available sooner
			sinks = []definition.Sink{*sink}
		}).
		AddFrom(r.
			rotateAllIn(rb, now, sinkKey, &sinks, openNewFile).
			AddProcessor(func(_ context.Context, result *op.Result[[]storage.File]) {
				// Unwrap results, there in only one file
				if result.Err() == nil {
					files := result.Result()
					if count := len(files); count == 1 {
						file = files[0]
					} else {
						result.AddErr(errors.Errorf(`expected 1 file, found %d`, count))
					}
				}
			}),
		)
}

// rotateAllIn is a common code used by rotate and close operations.
func (r *FileRepository) rotateAllIn(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sinksPtr *[]definition.Sink, openNew bool) *op.AtomicOp[[]storage.File] {
	// Validate arguments
	if openNew && rb == nil {
		panic(errors.New("rollback.Builder must be set if the creation of new file resources is allowed"))
	}

	// Init atomic operation
	var newFiles []storage.File
	atomicOp := op.Atomic(r.client, &newFiles)

	// Get sinks
	var sinks []definition.Sink
	if sinksPtr == nil {
		// Load sinks, if the slice is not provided externally
		if sinkKey, ok := parentKey.(key.SinkKey); ok {
			// Get
			atomicOp.ReadOp(r.all.sink.Get(sinkKey).WithOnResult(func(sink definition.Sink) {
				sinks = []definition.Sink{sink}
			}))
		} else {
			// List
			atomicOp.ReadOp(r.all.sink.List(parentKey).WithAllTo(&sinks))
		}
	} else {
		// Load sinks from the pointer, before write
		atomicOp.BeforeWrite(func(ctx context.Context) {
			sinks = *sinksPtr
		})
	}

	// Get sink keys
	var sinkKeys []key.SinkKey
	atomicOp.BeforeWrite(func(ctx context.Context) {
		for _, sink := range sinks {
			sinkKeys = append(sinkKeys, sink.SinkKey)
		}
	})

	// Get all active volumes
	var volumes []volume.Metadata
	atomicOp.ReadOp(r.all.Volume().ListWriterVolumes().WithAllTo(&volumes))

	// Create file resources
	var resources map[key.SinkKey]FileResource
	if openNew {
		provider := r.all.hook.NewFileResourcesProvider(rb)
		atomicOp.BeforeWriteOrErr(func(ctx context.Context) (err error) {
			resources, err = provider(ctx, now, sinkKeys)
			return err
		})
	}

	// Get disk space statistics to calculate pre-allocated disk space for a new slice
	var maxUsedDiskSpace map[key.SinkKey]datasize.ByteSize
	if openNew {
		provider := r.all.hook.NewUsedDiskSpaceProvider()
		atomicOp.BeforeWriteOrErr(func(ctx context.Context) (err error) {
			maxUsedDiskSpace, err = provider(ctx, sinkKeys)
			return err
		})
	}

	// Read opened files and slices.
	// There can be a maximum of one old file in the storage.FileWriting state per each table sink,
	// if present, it is atomically switched to the storage.FileClosing state.
	// Similarly, slices in the SliceWriting state are switched to the SliceClosing state.
	var openedFiles []storage.File
	var openedSlices []storage.Slice
	atomicOp.
		ReadOp(r.ListInState(parentKey, storage.FileWriting).WithAllTo(&openedFiles)).
		ReadOp(r.all.slice.ListInState(parentKey, storage.SliceWriting).WithAllTo(&openedSlices))

	// Close old files, open new files
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		txn := op.Txn(r.client)

		// Group opened files by the sink
		openedFilesPerSink := make(map[key.SinkKey][]storage.File)
		for _, file := range openedFiles {
			openedFilesPerSink[file.SinkKey] = append(openedFilesPerSink[file.SinkKey], file)
		}

		// Group opened slices by the file
		openedSlicesPerSink := make(map[key.SinkKey][]storage.Slice)
		for _, slice := range openedSlices {
			openedSlicesPerSink[slice.SinkKey] = append(openedSlicesPerSink[slice.SinkKey], slice)
		}

		// Open a new file in each sink
		errs := errors.NewMultiError()
		for _, sink := range sinks {
			// File should be opened only for the table sinks
			if sink.Type != definition.SinkTypeTable {
				continue
			}

			// Get file resource
			var resource FileResource
			if openNew {
				var ok bool
				if resource, ok = resources[sink.SinkKey]; !ok {
					errs.Append(errors.Errorf(`credentials for the sink "%s" was not provided`, sink.SinkKey))
					continue
				}
			}

			// Close the old file, if present
			sinkOpenedFiles := openedFilesPerSink[sink.SinkKey]
			if count := len(sinkOpenedFiles); count > 1 {
				errs.Append(errors.Errorf(`unexpected state, found %d opened files in the sink "%s"`, count, sink.SinkKey))
				continue
			} else if count == 1 {
				if oldFile := sinkOpenedFiles[0]; openNew && oldFile.FileKey == resource.FileKey {
					// File already exists
					return nil, serviceError.NewResourceAlreadyExistsError("file", oldFile.FileKey.String(), "sink")
				} else if modified, err := oldFile.WithState(now, storage.FileClosing); err == nil {
					// Switch the old file from the state storage.FileWriting to the state storage.FileClosing
					txn.Merge(r.updateTxn(oldFile, modified))
				} else {
					errs.Append(err)
				}
			}

			// Close old slices, if present
			for _, oldSlice := range openedSlicesPerSink[sink.SinkKey] {
				if modified, err := oldSlice.WithState(now, storage.SliceClosing); err == nil {
					// Switch the old slice from the state storage.SliceWriting to the state storage.SliceClosing
					txn.Merge(r.all.slice.updateTxn(oldSlice, modified))
				} else {
					errs.Append(err)
				}
			}

			// Open new file, if enabled
			if openNew {
				// Apply configuration patch from the sink to the global config
				cfg := r.config.With(sink.Table.Storage)

				// Create file entity
				file, err := newFile(cfg, resource, sink)
				if err != nil {
					errs.Append(err)
				}

				// Assign volumes
				file.Assignment = r.all.hook.AssignVolumes(ctx, volumes, cfg.VolumeAssignment, file.OpenedAt().Time())

				// At least one volume must be assigned
				if len(file.Assignment.Volumes) == 0 {
					errs.Append(errors.New(`no volume is available for the file`))
				}

				// Open slices in the assigned volumes
				for _, volumeID := range file.Assignment.Volumes {
					if slice, err := newSlice(now, file, volumeID, maxUsedDiskSpace[file.SinkKey]); err == nil {
						txn.Merge(r.all.slice.createTxn(slice))
					} else {
						errs.Append(err)
					}
				}

				// Open file
				txn.Merge(r.createTxn(file).OnResult(func(result *op.TxnResult[op.NoResult]) {
					if result.Succeeded() {
						newFiles = append(newFiles, file)
					}
				}))
			}
		}

		if err := errs.ErrorOrNil(); err != nil {
			return nil, err
		}

		if txn.Empty() {
			return nil, nil
		}

		return txn, nil
	})

	return atomicOp
}

// createTxn saves a new entity, see also update method.
// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
// - "All" prefix is used for classic CRUD operations.
// - "InLevel" prefix is used for effective watching of the storage level.
// nolint: dupl // similar code is in the SliceRepository
func (r *FileRepository) createTxn(value storage.File) *op.TxnOp[op.NoResult] {
	etcdKey := r.schema.AllLevels().ByKey(value.FileKey)
	return op.Txn(r.client).
		// Entity must not exist on create
		If(etcd.Compare(etcd.ModRevision(etcdKey.Key()), "=", 0)).
		AddProcessor(func(ctx context.Context, r *op.TxnResult[op.NoResult]) {
			if r.Err() == nil && !r.Succeeded() {
				r.AddErr(serviceError.NewResourceAlreadyExistsError("file", value.FileKey.String(), "sink"))
			}
		}).
		// Put entity to All and InLevel prefixes
		Then(etcdKey.Put(r.client, value)).
		Then(r.schema.InLevel(value.State.Level()).ByKey(value.FileKey).Put(r.client, value))
}

// updateTxn saves an existing entity, see also createTxn method.
func (r *FileRepository) updateTxn(oldValue, newValue storage.File) *op.TxnOp[op.NoResult] {
	txn := op.Txn(r.client)

	// Put entity to All and InLevel prefixes
	txn.
		Then(r.schema.AllLevels().ByKey(newValue.FileKey).Put(r.client, newValue)).
		Then(r.schema.InLevel(newValue.State.Level()).ByKey(newValue.FileKey).Put(r.client, newValue))

	// Delete entity from old level, if needed.
	if newValue.State.Level() != oldValue.State.Level() {
		txn.Then(r.schema.InLevel(oldValue.State.Level()).ByKey(oldValue.FileKey).Delete(r.client))
	}

	return txn
}

// readAndUpdate reads the file, applies updateFn and save modified value.
func (r *FileRepository) readAndUpdate(k storage.FileKey, updateFn func(storage.File) (storage.File, error)) *op.AtomicOp[storage.File] {
	var oldValue, newValue storage.File
	return op.Atomic(r.client, &newValue).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&oldValue)).
		// Prepare the new value
		BeforeWriteOrErr(func(context.Context) (err error) {
			newValue, err = updateFn(oldValue)
			return err
		}).
		// Save the updated object
		Write(func(context.Context) op.Op { return r.updateTxn(oldValue, newValue) })
}
