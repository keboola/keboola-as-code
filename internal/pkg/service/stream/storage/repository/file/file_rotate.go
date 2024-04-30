package file

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) rotateFileOnSinkModification() {
	r.plugins.Collection().OnSinkModification(func(ctx context.Context, now time.Time, by definition.By, old, sink *definition.Sink) {
		// Check is the sink type has support for files
		if !r.isSinkWithLocalStorage(sink) {
			return
		}

		// Rotate file, close active file, if any, and open a new file
		op.AtomicFromCtx(ctx).AddFrom(r.rotateOnModification(ctx, now, sink))
	})
}

// Rotate closes the opened file, if present, and opens a new file in the table sink.
//   - The old file, if present, is switched from the model.FileWriting state to the model.FileClosing state.
//   - New file in the model.FileWriting state is created.
//   - This method is used to rotate files when the import conditions are met.
func (r *Repository) Rotate(k key.SinkKey, now time.Time) *op.AtomicOp[model.File] {
	return r.rotate(now, k, nil, nil) // load source and sink from database
}

func (r *Repository) rotateOnModification(ctx context.Context, now time.Time, sink *definition.Sink) *op.AtomicOp[model.File] {
	return r.rotate(now, sink.SinkKey, plugin.SourceFromContext(ctx), sink)
}

func (r *Repository) rotate(now time.Time, k key.SinkKey, source *definition.Source, sink *definition.Sink) *op.AtomicOp[model.File] {
	// Create atomic operation
	var openedFile model.File
	atomicOp := op.Atomic(r.client, &openedFile)

	// Load Sink entity, if needed
	if sink == nil {
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.definition.Sink().Get(k).WithResultTo(sink)
		})
	}

	// Open a new file
	atomicOp.AddFrom(r.openSink(now, source, *sink).SetResultTo(&openedFile))

	// Close active files
	atomicOp.AddFrom(r.closeSink(now, *sink))

	return atomicOp
}
