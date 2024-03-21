package file

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/file/schema"
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
			ctx.AddAtomicOp(r.Rotate(updated.SinkKey, ctx.Now()))
		} else if deactivated {
			// Close file on the sink deactivation
			ctx.AddAtomicOp(r.Close(updated.SinkKey, ctx.Now()))
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

// Rotate closes the opened file, if present, and opens a new file in the table sink.
//   - The old file, if present, is switched from the model.FileWriting state to the model.FileClosing state.
//   - New file in the model.FileWriting state is created.
//   - This method is used to rotate files when the import conditions are met.
func (r *Repository) Rotate(k key.SinkKey, now time.Time) *op.AtomicOp[model.File] {
	return r.rotate(k, now, true)
}

// Close closes opened file in the sink.
// - NO NEW FILE is created, so the sink stops accepting new writes, that's the difference with RotateAllIn.
// - THE OLD FILE in the model.FileWriting state, IF PRESENT, is switched to the model.FileClosing state.
// - This method is used on Sink/Source soft-delete or disable operation.
func (r *Repository) Close(k key.SinkKey, now time.Time) *op.AtomicOp[op.NoResult] {
	// There is no result of the operation, no new file is opened.
	return op.
		Atomic(r.client, &op.NoResult{}).
		AddFrom(r.rotate(k, now, false))
}

// IncrementRetry increments retry attempt and backoff delay on an error.
// Retry is reset on StateTransition.
func (r *Repository) IncrementRetry(k model.FileKey, now time.Time, reason string) *op.AtomicOp[model.File] {
	return r.update(k, now, func(slice model.File) (model.File, error) {
		slice.IncrementRetry(r.backoff, now, reason)
		return slice, nil
	})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *Repository) StateTransition(k model.FileKey, now time.Time, from, to model.FileState) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		// File should be closed via one of the following ways:
		//   - Rotate* methods - to create new replacement files
		//   - Close* methods - no replacement files are created.
		//   - Therefore, closing file via the StateTransition method is forbidden.
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
}

// Delete the file.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *Repository) Delete(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.update(k, now, func(file model.File) (model.File, error) {
		file.Deleted = true
		return file, nil
	})
}

// rotateAllIn is a common method used by both Rotate and Close method.
//
// If openNew is set to true, the operation will open new files and slices; if false, it will only close the existing ones.
func (r *Repository) rotate(k key.SinkKey, now time.Time, openNewFile bool) *op.AtomicOp[model.File] {
	// Init atomic operation
	var openedFile model.File
	atomicOp := op.Atomic(r.client, &openedFile)

	// Load source to get configuration patch
	var source definition.Source
	atomicOp.ReadOp(r.definition.Source().Get(k.SourceKey).WithResultTo(&source))

	// Load sink
	var sink definition.Sink
	atomicOp.ReadOp(r.definition.Sink().Get(k).WithResultTo(&sink))

	// Get all active volumes
	var volumes []volume.Metadata
	if openNewFile {
		atomicOp.ReadOp(r.volumes.ListWriterVolumes().WithAllTo(&volumes))
	}

	// Load opened files in the model.FileWriting state.
	// There can be a maximum of one old file in the model.FileWriting state per each table sink.
	// On rotation, the opened file is switched to the model.FileClosing state.
	var openedFiles []model.File
	atomicOp.ReadOp(r.ListInState(k, model.FileWriting).WithAllTo(&openedFiles))

	// Close old file, open new file
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		// File should be opened only for the table sinks
		if sink.Type != definition.SinkTypeTable {
			return nil, nil
		}

		// There must be at most one opened file in the sink
		filesCount := len(openedFiles)
		if filesCount > 1 {
			return nil, errors.Errorf(`unexpected state, found %d opened files in the sink "%s"`, filesCount, sink.SinkKey)
		}

		saveCtx := plugin.NewSaveContext(now)

		// Close the old file, if present
		if filesCount == 1 {
			// Switch the old file from the state model.FileWriting to the state model.FileClosing
			oldFile := openedFiles[0]
			oldUpdatedFile, err := oldFile.WithState(now, model.FileClosing)
			if err != nil {
				return nil, err
			}

			// Save update old file
			r.save(saveCtx, &oldFile, &oldUpdatedFile)
		}

		// Open new file, if enabled
		if openNewFile {
			// Apply configuration overrides from the source and the sink
			cfg := r.config
			patch := level.ConfigPatch{}
			for _, kvs := range []configpatch.PatchKVs{source.Config, sink.Config} {
				err := configpatch.ApplyKVs(&cfg, &patch, kvs.In("storage.level"), configpatch.WithModifyProtected())
				if err != nil {
					return nil, err
				}
			}

			// Create file entity
			fileKey := model.FileKey{SinkKey: sink.SinkKey, FileID: model.FileID{OpenedAt: utctime.From(now)}}
			if newFile, err := NewFile(cfg, fileKey, sink); err == nil {
				openedFile = newFile
			} else {
				return nil, err
			}

			// Assign volumes
			openedFile.Assignment = r.volumes.AssignVolumes(volumes, cfg.Local.Volume.Assignment, openedFile.OpenedAt().Time())

			// At least one volume must be assigned
			if len(openedFile.Assignment.Volumes) == 0 {
				return nil, errors.New(`no volume is available for the file`)
			}

			// Save new file
			r.save(saveCtx, nil, &openedFile)
		}

		return saveCtx.Apply(ctx)
	})

	return atomicOp
}

// update reads the file, applies updateFn and save modified value.
func (r *Repository) update(k model.FileKey, now time.Time, updateFn func(model.File) (model.File, error)) *op.AtomicOp[model.File] {
	var old, updated model.File
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Update
			updated = deepcopy.Copy(old).(model.File)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			return r.saveOne(ctx, now, &old, &updated)
		})
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *model.File) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Apply(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *model.File) {
	// Call plugins
	r.plugins.Executor().OnFileSave(saveCtx, old, updated)

	allKey := r.schema.AllLevels().ByKey(updated.FileKey)
	inLevelKey := r.schema.InLevel(updated.State.Level()).ByKey(updated.FileKey)

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
					r.AddErr(serviceError.NewResourceAlreadyExistsError("file", updated.FileKey.String(), "sink"))
				}),
			)
		} else {
			// Entity should exist
			saveCtx.AddOp(op.Txn(r.client).
				If(etcd.Compare(etcd.ModRevision(allKey.Key()), "!=", 0)).
				OnFailed(func(r *op.TxnResult[op.NoResult]) {
					r.AddErr(serviceError.NewResourceNotFoundError("file", updated.FileKey.String(), "sink"))
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
				r.schema.InLevel(old.State.Level()).ByKey(old.FileKey).Delete(r.client),
			)
		}
	}
}
