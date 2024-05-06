package file

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (r *Repository) SwitchToImporting(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.stateTransition(k, now, model.FileClosing, model.FileImporting)
}

func (r *Repository) SwitchToImported(k model.FileKey, now time.Time) *op.AtomicOp[model.File] {
	return r.stateTransition(k, now, model.FileImporting, model.FileImported)
}

// stateTransition switch state of the file, state of the file slices is also atomically switched using plugin, if needed.
func (r *Repository) stateTransition(k model.FileKey, now time.Time, from, to model.FileState) *op.AtomicOp[model.File] {
	var old, updated model.File
	return op.Atomic(r.client, &updated).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		Write(func(ctx context.Context) op.Op {
			return r.switchState(ctx, old, now, from, to).SetResultTo(&updated)
		})
}

func (r *Repository) switchState(ctx context.Context, oldValue model.File, now time.Time, from, to model.FileState) *op.TxnOp[model.File] {
	// Validate from state
	if oldValue.State != from {
		return op.TxnWithError[model.File](errors.Errorf(`file "%s" is in "%s" state, expected "%s"`, oldValue.FileKey, oldValue.State, from))
	}

	// Switch file state
	newValue, err := oldValue.WithState(now, to)
	if err != nil {
		return op.TxnWithError[model.File](err)
	}

	return r.save(ctx, now, &oldValue, &newValue)
}

func (r *Repository) switchStateInBatch(ctx context.Context, original []model.File, now time.Time, from, to model.FileState) *op.TxnOp[[]model.File] {
	var updated []model.File
	txn := op.TxnWithResult(r.client, &updated)
	for _, file := range original {
		txn.Merge(r.switchState(ctx, file, now, from, to).OnSucceeded(func(r *op.TxnResult[model.File]) {
			updated = append(updated, r.Result())
		}))
	}
	return txn
}
