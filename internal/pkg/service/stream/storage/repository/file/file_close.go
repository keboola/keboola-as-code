package file

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) closeFileOnSinkDeactivation() {
	r.plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if r.plugins.IsSinkWithLocalStorage(sink) {
			op.AtomicOpFromCtx(ctx).AddFrom(r.closeFilesInSink(sink.SinkKey, now))
		}
		return nil
	})
}

// closeFilesInSink - closes active files, in the FileWriting state, in the sink.
// There should be at most one active file in each sink.
// Files are switched to the FileClosing state.
func (r *Repository) closeFilesInSink(k key.SinkKey, now time.Time) *op.AtomicOp[[]model.File] {
	var files, closedFiles []model.File
	return op.Atomic(r.client, &closedFiles).
		// Load active files
		Read(func(ctx context.Context) op.Op {
			return r.ListInState(k, model.FileWriting).WithAllTo(&files)
		}).
		// Close active files
		Write(func(ctx context.Context) op.Op {
			// There should be a maximum of one old file in the model.FileWriting state per each table sink.
			// Log error and close all found files.
			if filesCount := len(files); filesCount > 1 {
				r.logger.Errorf(ctx, `unexpected state, found %d opened files in the sink "%s"`, filesCount, k)
			}

			// Close
			return r.
				switchStateInBatch(ctx, files, now, model.FileWriting, model.FileClosing).
				SetResultTo(&closedFiles)
		})
}
