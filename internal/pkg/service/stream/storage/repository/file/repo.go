package file

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file/schema"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	volumeRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Repository provides database operations with the model.File entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type Repository struct {
	client     etcd.KV
	schema     schema.File
	config     level.Config
	backoff    model.RetryBackoff
	volumes    *volumeRepo.Repository
	plugins    *plugin.Plugins
	definition *definitionRepo.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	DefinitionRepository() *definitionRepo.Repository
}

// rotateSinkContext is an auxiliary struct to group arguments needed to rotate a Sink.
type rotateSinkContext struct {
	// Now timestamp, common for all parts of the atomic operation
	Now time.Time
	// OpenNew files or just close old ones?
	OpenNew bool
	// Sink, parent of files and slices
	Sink definition.Sink
	// Volumes provides all active volumes
	Volumes []volume.Metadata
	// OpenedFiles in the model.FileWriting state to be closed, maximum one file
	OpenedFiles []model.File
	// OpenedSlices in the model.SliceWriting state to be closed
	OpenedSlices []model.Slice
}

func NewRepository(cfg level.Config, d dependencies, backoff model.RetryBackoff, volumes *volumeRepo.Repository) *Repository {
	return &Repository{
		client:     d.EtcdClient(),
		schema:     schema.ForFile(d.EtcdSerde()),
		config:     cfg,
		backoff:    backoff,
		volumes:    volumes,
		plugins:    d.Plugins(),
		definition: d.DefinitionRepository(),
	}
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

// Rotate closes the opened file, if present, and opens a new file in the table sink.
//   - The old file, if present, is switched from the model.FileWriting state to the model.FileClosing state.
//   - New file in the model.FileWriting is created.
//   - AllKVs file slices in the model.SliceWriting state are switched to the model.SliceClosing state.
//   - Opening new slices in the file, on different volumes, is not the task of this method.
//   - Files rotation is done atomically.
//   - This method is used to rotate files when the import conditions are met.
func (r *Repository) Rotate(now time.Time, sinkKey key.SinkKey) *op.AtomicOp[model.File] {
	return r.rotate(now, sinkKey, true)
}

// RotateAllIn is same as Rotate method, but it is applied for each table sink within the parentKey.
// - This method is used on Sink/Source undelete or enable operation.
func (r *Repository) RotateAllIn(now time.Time, parentKey fmt.Stringer) *op.AtomicOp[[]model.File] {
	return r.rotateAllIn(now, parentKey, true)
}

// CloseAllIn closes opened file in each table sink within the parentKey.
// - NO NEW FILE is created, so the sink stops accepting new writes, that's the difference with RotateAllIn.
// - THE OLD FILE in the model.FileWriting state, IF PRESENT, is switched to the model.FileClosing state.
// - Files closing is done atomically.
// - This method is used on Sink/Source soft-delete or disable operation.
func (r *Repository) CloseAllIn(now time.Time, parentKey fmt.Stringer) *op.AtomicOp[op.NoResult] {
	// There is no result of the operation, no new file is opened.
	return op.
		Atomic(r.client, &op.NoResult{}).
		AddFrom(r.rotateAllIn(now, parentKey, false))
}

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *Repository) IncrementRetry(now time.Time, k model.FileKey, reason string) *op.AtomicOp[model.File] {
	return r.
		update(k, func(slice model.File) (model.File, error) {
			slice.IncrementRetry(r.backoff, now, reason)
			return slice, nil
		})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *Repository) StateTransition(now time.Time, fileKey model.FileKey, from, to model.FileState) *op.AtomicOp[model.File] {
	//var fileSlices []model.Slice
	atomicOp := r.
		// Modify the file
		update(fileKey, func(file model.File) (model.File, error) {
			// File should be closed via one of the following ways:
			//   - Rotate* methods - to create new replacement files
			//   - Close* methods - no replacement files are created.
			//   - Closing file via StateTransition is therefore forbidden.
			if to == model.FileClosing {
				return model.File{}, errors.Errorf(`unexpected file transition to the state "%s", use Rotate* or Close* methods`, model.FileClosing)
			}

			// Validate from state
			if file.State != from {
				return model.File{}, errors.Errorf(`file "%s" is in "%s" state, expected "%s"`, file.FileKey, file.State, from)
			}

			// Switch file state
			return file.WithState(now, to)
		})
	//// Read slices for modification
	//ReadOp(r.all.slice.ListIn(fileKey).WithAllTo(&fileSlices)).
	//// Modify slices states, if needed
	//WriteOrErr(func(context.Context) (out op.Op, err error) {
	//	txn := op.Txn(r.client)
	//	errs := errors.NewMultiError()
	//	for _, slice := range fileSlices {
	//		oldSliceState := slice
	//		if to == model.FileClosing && slice.State == model.SliceWriting {
	//			// Switch slice state on FileClosing
	//			if slice, err = slice.WithState(now, model.SliceClosing); err != nil {
	//				errs.Append(err)
	//				continue
	//			}
	//		} else if to == model.FileImported && slice.State == model.SliceUploaded {
	//			// Switch slice state on FileImported
	//			if slice, err = slice.WithState(now, model.SliceImported); err != nil {
	//				errs.Append(err)
	//				continue
	//			}
	//		}
	//
	//		// Validate file and slice state combination
	//		if err = repository.validateFileAndSliceStates(to, slice.State); err != nil {
	//			return nil, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
	//		}
	//
	//		// Save modified value, if there is a difference
	//		if !reflect.DeepEqual(oldSliceState, slice) {
	//			txn.Merge(r.all.slice.updateTxn(oldSliceState, slice))
	//		}
	//	}
	//
	//	if err = errs.ErrorOrNil(); err != nil {
	//		return nil, err
	//	}
	//
	//	if !txn.Empty() {
	//		return txn, nil
	//	}
	//
	//	return nil, nil
	//})

	return atomicOp
}

// Delete file a file slices.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *Repository) Delete(k model.FileKey) *op.AtomicOp[op.NoResult] {
	atomicOp := op.Atomic(r.client, &op.NoResult{})

	// Delete entity from All prefix
	atomicOp.WriteOp(
		r.schema.
			AllLevels().ByKey(k).DeleteIfExists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("file", k.String(), "sink")
			}),
	)

	// Delete entity from InLevel prefixes
	for _, l := range level.AllLevels() {
		atomicOp.WriteOp(r.schema.InLevel(l).ByKey(k).Delete(r.client))
	}

	return atomicOp
}

// rotate one file, it is a special case of the rotateAllIn.
func (r *Repository) rotate(now time.Time, sinkKey key.SinkKey, openNewFile bool) *op.AtomicOp[model.File] {
	var file model.File
	return op.Atomic(r.client, &file).
		AddFrom(r.
			rotateAllIn(now, sinkKey, openNewFile).
			AddProcessor(func(_ context.Context, result *op.Result[[]model.File]) {
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

// rotateAllIn is a common function used by both rotate and close operations.
//
// The now represents a common timestamp for all operations within the AtomicOp.
//
// The rb rollback.Builder is used to undo the creation of file resources in the Storage API if the operation fails.
//
// The parentKey identifies the group in which files and slices will be rotated.
// Expected values include: keboola.ProjectID, key.BranchKey, key.SourceKey, and key.SinkKey.
//
// The sinks pointer refers to a slice of sinks that should be provided before the write phase of the AtomicOp.
// This allows to provide already loaded sinks, or to implement their loading in the parent code, in the read phase.
// If the pointer is nil, the loading of sinks is handled automatically.
//
// If openNew is set to true, the operation will open new files and slices; if false, it will only close the existing ones.
func (r *Repository) rotateAllIn(now time.Time, parentKey fmt.Stringer, openNew bool) *op.AtomicOp[[]model.File] {
	// Validate arguments
	if openNew && rb == nil {
		panic(errors.New("rollback.Builder must be set if the creation of new file resources is allowed"))
	}

	// Init atomic operation
	var newFiles []model.File
	atomicOp := op.Atomic(r.client, &newFiles)

	// Load sinks
	var sinks []definition.Sink
	if sinkKey, ok := parentKey.(key.SinkKey); ok {
		// Get
		atomicOp.ReadOp(r.definition.Sink().Get(sinkKey).WithOnResult(func(sink definition.Sink) {
			sinks = []definition.Sink{sink}
		}))
	} else {
		// List
		atomicOp.ReadOp(r.definition.Sink().List(parentKey).WithAllTo(&sinks))
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
	if openNew {
		atomicOp.ReadOp(r.volumes.ListWriterVolumes().WithAllTo(&volumes))
	}

	//// Create file resources
	//var fileResources map[key.SinkKey]*repository.FileResource
	//if openNew {
	//	provider := r.all.external.NewFileResourcesProvider(rb)
	//	atomicOp.BeforeWriteOrErr(func(ctx context.Context) (err error) {
	//		fileResources, err = provider(ctx, now, sinkKeys)
	//		return err
	//	})
	//}

	// Read opened files in the model.FileWriting state.
	// There can be a maximum of one old file in the model.FileWriting state per each table sink.
	// On rotation, opened files are switched to the model.FileClosing state.
	var openedFiles []model.File
	atomicOp.ReadOp(r.ListInState(parentKey, model.FileWriting).WithAllTo(&openedFiles))

	//// Read opened slices in the model.SliceWriting state.
	//// On rotation, opened slices are switched to the model.SliceClosing state.
	//var openedSlices []model.Slice
	//atomicOp.ReadOp(r.all.slice.ListInState(parentKey, model.SliceWriting).WithAllTo(&openedSlices))

	// Group opened files by the sink
	var openedFilesPerSink map[key.SinkKey][]model.File
	atomicOp.BeforeWrite(func(ctx context.Context) {
		openedFilesPerSink = make(map[key.SinkKey][]model.File)
		for _, file := range openedFiles {
			openedFilesPerSink[file.SinkKey] = append(openedFilesPerSink[file.SinkKey], file)
		}
	})

	//// Group opened slices by the file
	//var openedSlicesPerSink map[key.SinkKey][]model.Slice
	//atomicOp.BeforeWrite(func(ctx context.Context) {
	//	openedSlicesPerSink = make(map[key.SinkKey][]model.Slice)
	//	for _, slice := range openedSlices {
	//		openedSlicesPerSink[slice.SinkKey] = append(openedSlicesPerSink[slice.SinkKey], slice)
	//	}
	//})

	// Close old files, open new files
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		txn := op.Txn(r.client)
		errs := errors.NewMultiError()

		// Open a new file in each sink
		for _, sink := range sinks {
			sinkTxn, err := r.rotateSink(ctx, rotateSinkContext{
				Now:         now,
				OpenNew:     openNew,
				Sink:        sink,
				Volumes:     volumes,
				OpenedFiles: openedFilesPerSink[sink.SinkKey],
				//OpenedSlices:    openedSlicesPerSink[sink.SinkKey],
			})

			if err != nil {
				errs.Append(err)
			} else if sinkTxn != nil {
				txn.Merge(sinkTxn.OnSucceeded(func(r *op.TxnResult[*model.File]) {
					if f := r.Result(); f != nil {
						newFiles = append(newFiles, *f)
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

func (r *Repository) rotateSink(ctx context.Context, c rotateSinkContext) (*op.TxnOp[*model.File], error) {
	// File should be opened only for the table sinks
	if c.Sink.Type != definition.SinkTypeTable {
		return nil, nil
	}

	// Create txn
	var result *model.File
	txn := op.TxnWithResult(r.client, &result)

	// Close the old file, if present
	if count := len(c.OpenedFiles); count > 1 {
		return nil, errors.Errorf(`unexpected state, found %d opened files in the sink "%s"`, count, c.Sink.SinkKey)
	} else if count == 1 {
		// Switch the old file from the state model.FileWriting to the state model.FileClosing
		modified, err := c.OpenedFiles[0].WithState(c.Now, model.FileClosing)
		if err != nil {
			return nil, err
		}


		if ; err == nil {

			txn.Merge(r.updateTxn(oldFile, modified))
		} else {
			return nil, err
		}
	}

	//// Close old slices, if present
	//for _, oldSlice := range c.OpenedSlices {
	//	if modified, err := oldSlice.WithState(c.Now, model.SliceClosing); err == nil {
	//		// Switch the old slice from the state model.SliceWriting to the state model.SliceClosing
	//		txn.Merge(r.all.slice.updateTxn(oldSlice, modified))
	//	} else {
	//		return nil, err
	//	}
	//}

	// Open new file, if enabled
	if c.NewFileResource != nil {
		// Apply configuration patch from the sink to the global config
		cfg := r.config
		err := configpatch.ApplyKVs(
			&cfg,
			&level.ConfigPatch{},
			c.Sink.Config.In("storage.level"),
			configpatch.WithModifyProtected(), // this validation is performed of the config patch saving
		)
		if err != nil {
			return nil, err
		}

		// Create file entity
		file, err := NewFile(cfg, fileKey, c.Sink)
		if err != nil {
			return nil, err
		}

		// Assign volumes
		file.Assignment = r.volumes.AssignVolumes(c.Volumes, cfg.Local.Volume.Assignment, file.OpenedAt().Time())

		// At least one volume must be assigned
		if len(file.Assignment.Volumes) == 0 {
			return nil, errors.New(`no volume is available for the file`)
		}

		// Open file
		txn.Merge()
		txn.Merge(r.createTxn(file).OnSucceeded(func(r *op.TxnResult[model.File]) {
			file = r.Result()
			result = &file
		}))
	}

	return txn, nil
}

// update reads the file, applies updateFn and save modified value.
func (r *Repository) update(k model.FileKey, now time.Time, updateFn func(model.File) (model.File, error)) *op.AtomicOp[model.File] {
	var oldValue, newValue model.File
	return op.Atomic(r.client, &newValue).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&oldValue)).
		// Prepare the new value
		BeforeWriteOrErr(func(context.Context) (err error) {
			newValue, err = updateFn(oldValue)
			return err
		}).
		// Save the entity
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			return r.saveOne(ctx, now, &newValue)
		})
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, v *model.File) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, now, v)
	return saveCtx.Apply(ctx)
}

func (r *Repository) saveAll(ctx context.Context, now time.Time, all []model.File) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	for i := range all {
		r.save(saveCtx, now, &all[i])
	}
	return saveCtx.Apply(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, now time.Time, v *model.File) {
	// Call plugins
	r.plugins.Executor().OnFileSave(saveCtx, v)

	allKey := r.schema.AllLevels().ByKey(v.FileKey)
	inLevelKey := r.schema.InLevel(v.State.Level()).ByKey(v.FileKey)

	if v.Deleted {
		// Delete entity to All and InLevel prefixes
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
					r.AddErr(serviceError.NewResourceAlreadyExistsError("file", v.FileKey.String(), "sink"))
				}),
			)
		} else {
			// Entity should exist
			saveCtx.AddOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("file", v.FileKey.String(), "sink"))
				}),
			)
		}

		// Put entity to All and InLevel prefixes
		saveCtx.AddOp(
			allKey.Put(r.client, *v),
			inLevelKey.Put(r.client, *v),
		)

		// Remove entity from the old InLevel prefix, if needed
		if {
			saveCtx.AddOp(
				r.schema.InLevel(v.State.Level()).ByKey(v.FileKey).Delete(r.client),
			)
		}
	}
}
