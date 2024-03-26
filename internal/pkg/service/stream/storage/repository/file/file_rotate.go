package file

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

func (r *Repository) rotateFileOnSinkModification() {
	r.plugins.Collection().OnSinkModification(func(ctx *plugin.SaveContext, old, updated *definition.Sink) {

	})
}

// Rotate closes the opened file, if present, and opens a new file in the table sink.
//   - The old file, if present, is switched from the model.FileWriting state to the model.FileClosing state.
//   - New file in the model.FileWriting state is created.
//   - This method is used to rotate files when the import conditions are met.
func (r *Repository) Rotate(k key.SinkKey, now time.Time) *op.AtomicOp[model.File] {
	return r.rotate(k, now, true)
}

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

		return saveCtx.Do(ctx)
	})

	return atomicOp
}
