package slice

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) deleteSlicesOnFileDelete() {
	r.plugins.Collection().OnFileDelete(func(ctx context.Context, now time.Time, original, file *model.File) error {
		return r.deleteAllInBatches(ctx, file.FileKey, now)
	})
}

// deleteAllInBatches soft-deletes all slices of the file in bounded transactions.
//
// A file can accumulate many slices (e.g. a throttled file that keeps getting new slices), and
// deleting them all in a single transaction would exceed the etcd per-transaction operation limit.
// The slices are therefore deleted in batches, each batch in its own transaction.
//
// This callback runs during generation of the file delete transaction, so the slice batches commit
// BEFORE the file delete commits - deliberately "children first". If the file delete then
// collides/retries/fails, the file stays present with a subset of its slices deleted: an incomplete
// delete that the cleanup node retries (the file is still present and still expired), re-listing and
// deleting only the remaining slices. Slice deletion is monotonic and idempotent, the file is locked
// by the cleanup node during the delete, and the data is old (no cross-batch strong consistency is
// required), so the intermediate state is observable but not corrupt.
//
// This operation deletes only the metadata; the file resources in the local or staging storage are
// unaffected.
func (r *Repository) deleteAllInBatches(ctx context.Context, k model.FileKey, now time.Time) error {
	var slices []model.Slice
	if err := r.ListIn(k).WithAllTo(&slices).Do(ctx).Err(); err != nil {
		return err
	}

	// Each slice delete is 2 ops (removal from the All and InLevel prefixes), so size the batch to
	// stay under the per-transaction limit.
	batchSize := op.MaxOpsPerTxn / 2
	for start := 0; start < len(slices); start += batchSize {
		batch := slices[start:min(start+batchSize, len(slices))]

		// Each batch runs in its own AtomicOp so the slice save plugins (which require the atomic
		// operation context) are satisfied; the write phase generates no extra operations because a
		// delete-save has no IF condition and no plugin reacts to a slice deletion.
		err := op.Atomic(r.client, &op.NoResult{}).
			Write(func(ctx context.Context) op.Op {
				return r.updateAll(ctx, now, batch, func(slice model.Slice) (model.Slice, error) {
					slice.Deleted = true
					return slice, nil
				})
			}).
			Do(ctx).Err()
		if err != nil {
			return err
		}
	}

	return nil
}
