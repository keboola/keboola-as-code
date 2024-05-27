package file

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// Rotate closes the active file, if present, and opens a new file for the sink.
//   - The old active file, if present, is switched from the model.FileWriting state to the model.FileClosing state.
//   - New file in the model.FileWriting state is created.
//   - This method is used to rotate file when the import conditions are met.
func (r *Repository) Rotate(k key.SinkKey, now time.Time) *op.AtomicOp[model.File] {
	return r.rotate(now, k, nil, nil) // load source and sink from database
}

func (r *Repository) rotateFileOnSinkModification() {
	r.plugins.Collection().OnSinkModification(func(ctx context.Context, now time.Time, by definition.By, old, sink *definition.Sink) error {
		// Check is the sink type has support for files
		if !r.plugins.IsSinkWithLocalStorage(sink) {
			return nil
		}

		// Rotate file, close active file, if any, and open a new file
		op.AtomicOpFromCtx(ctx).AddFrom(r.rotate(now, sink.SinkKey, plugin.SourceFromContext(ctx, sink.SourceKey), sink))

		return nil
	})
}

func (r *Repository) rotate(now time.Time, k key.SinkKey, source *definition.Source, sink *definition.Sink) *op.AtomicOp[model.File] {
	// Create atomic operation
	var openedFile model.File
	atomicOp := op.Atomic(r.client, &openedFile)

	// Open a new file
	atomicOp.AddFrom(r.openFileForSink(k, now, source, sink).SetResultTo(&openedFile))

	// Close active files
	atomicOp.AddFrom(r.closeFilesInSink(k, now))

	return atomicOp
}
