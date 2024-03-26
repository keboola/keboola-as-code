package file

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

func (r *Repository) rotateFileOnSinkModification() {
	r.plugins.Collection().OnSinkModification(func(ctx *plugin.SaveContext, old, updated *definition.Sink) {
		ctx.AddFrom(r.Rotate(updated.SinkKey, ctx.Now()))
	})
}

// Rotate closes the opened file, if present, and opens a new file in the table sink.
//   - The old file, if present, is switched from the model.FileWriting state to the model.FileClosing state.
//   - New file in the model.FileWriting state is created.
//   - This method is used to rotate files when the import conditions are met.
func (r *Repository) Rotate(k key.SinkKey, now time.Time) *op.AtomicOp[model.File] {
	// Create atomic operation
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
	atomicOp.ReadOp(r.volumes.ListWriterVolumes().WithAllTo(&volumes))

	// Load active files in the model.FileWriting state.
	// There can be a maximum of one old file in the model.FileWriting state per each table sink.
	// On rotation, the opened file is switched to the model.FileClosing state.
	var activeFiles []model.File
	atomicOp.ReadOp(r.ListInState(k, model.FileWriting).WithAllTo(&activeFiles))

	// Close old file, open new file
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		// File should be opened only for the table sinks
		if !r.isSinkWithLocalStorage(sink) {
			return nil, nil
		}

		// There must be at most one opened file in the sink
		filesCount := len(activeFiles)
		if filesCount > 1 {
			return nil, errors.Errorf(`unexpected state, found %d opened files in the sink "%s"`, filesCount, sink.SinkKey)
		}

		saveCtx := plugin.NewSaveContext(now)

		// Close the old file, if present
		if filesCount == 1 {
			if _, err := r.close(saveCtx, activeFiles[0]); err != nil {
				return nil, err
			}
		}

		// Open new file
		if f, err := r.open(saveCtx, source, sink, volumes); err == nil {
			openedFile = f
		} else {
			return nil, err
		}

		return saveCtx.Do(ctx)
	})

	return atomicOp
}
