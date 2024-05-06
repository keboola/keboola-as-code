package file

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) closeFileOnSinkDeactivation() {
	r.plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) {
		// Check is the sink type has support for files
		if !r.isSinkWithLocalStorage(sink) {
			return
		}

		// Close active file, if any
		op.AtomicFromCtx(ctx).AddFrom(r.closeFileInSink(now, sink.SinkKey))
	})
}

func (r *Repository) closeFileInSink(now time.Time, k key.SinkKey) *op.AtomicOp[[]model.File] {
	var closedFiles []model.File
	atomicOp := op.Atomic(r.client, &closedFiles)

	// Load active files in the model.FileWriting state.
	var activeFiles []model.File
	atomicOp.ReadOp(r.ListInState(k, model.FileWriting).WithAllTo(&activeFiles))

	// Close active files
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		// There should be a maximum of one old file in the model.FileWriting state per each table sink.
		// Log error and close all found files.
		if filesCount := len(activeFiles); filesCount > 1 {
			r.logger.Errorf(ctx, `unexpected state, found %d opened files in the sink "%s"`, filesCount, k)
		}

		// Close
		txn := op.Txn(r.client)
		for _, activeFile := range activeFiles {
			if closeTxn, err := r.closeFile(ctx, now, activeFile); err == nil {
				closedFiles = append(closedFiles, activeFile)
				txn.Merge(closeTxn)
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

func (r *Repository) closeFile(ctx context.Context, now time.Time, file model.File) (*op.TxnOp[model.File], error) {
	// Switch the old file from the state model.FileWriting to the state model.FileClosing
	updated, err := file.WithState(now, model.FileClosing)
	if err != nil {
		return nil, err
	}

	// Save update old file
	return r.save(ctx, now, &file, &updated), nil
}
