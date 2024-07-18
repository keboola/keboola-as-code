package repository

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) estimateSliceSizeOnSliceOpen() {
	r.plugins.Collection().OnSliceOpen(func(ctx context.Context, now time.Time, file model.File, slice *model.Slice) error {
		// Optional: If Sink is new, we can skip calculation, there is no previous slice/statistics
		if sink := plugin.SinkFromContext(ctx, file.SinkKey); sink != nil && sink.CreatedAt().Time().Equal(now) {
			slice.LocalStorage.AllocatedDiskSpace = file.LocalStorage.Allocation.ForNextSlice(0)
			return nil
		}

		// The operation is performed immediately, outside the atomic operation!
		// We need to update the value to the Slice entity before saving,
		// before the callback is completed, because later the entity value is already stored
		// in the WRITE phase transaction and cannot be modified.
		if err := r.estimateSliceSize(ctx, file, slice); err != nil {
			// Error is not fatal
			r.logger.Errorf(ctx, `cannot calculate slice pre-allocated size: %s`, err)
		}
		return nil
	})
}

func (r *Repository) estimateSliceSize(ctx context.Context, file model.File, slice *model.Slice) error {
	// Get maximum slice size
	size, err := r.maxUsedDiskSizeBySliceIn(ctx, slice.SinkKey, recordsForSliceDiskSizeCalc)
	if err != nil {
		return err
	}

	// Calculate allocated disk space for the new slice
	slice.LocalStorage.AllocatedDiskSpace = file.LocalStorage.Allocation.ForNextSlice(size)
	return nil
}

// maxUsedDiskSizeBySliceIn scans the statistics in the parentKey, scanned are:
//   - The last <limit> slices in level.LevelStaging (uploaded slices).
//   - The last <limit> slices in level.LevelTarget  (imported slices).
func (r *Repository) maxUsedDiskSizeBySliceIn(ctx context.Context, parentKey fmt.Stringer, limit int) (datasize.ByteSize, error) {
	// Get last <limit> slices from staging and target levels
	var lastStagingSlices, lastTargetSlices []model.Slice
	listSlicesOpts := []iterator.Option{iterator.WithSort(etcd.SortDescend), iterator.WithLimit(limit)}
	err := op.Txn(r.client).
		Then(r.storage.Slice().ListInLevel(parentKey, model.LevelStaging, listSlicesOpts...).WithAllTo(&lastStagingSlices)).
		Then(r.storage.Slice().ListInLevel(parentKey, model.LevelTarget, listSlicesOpts...).WithAllTo(&lastTargetSlices)).
		Do(ctx).
		Err()
	if err != nil {
		return 0, err
	}

	// Load and process statistics for each slice from the previous step
	var maxSize datasize.ByteSize
	txn := op.Txn(r.client)
	for _, slice := range slices.Concat(lastStagingSlices, lastTargetSlices) {
		txn.Merge(
			r.AggregateInLevel(slice.SliceKey, slice.State.Level()).OnSucceeded(func(r *op.TxnResult[statistics.Aggregated]) {
				if s := r.Result().Total.CompressedSize; s > maxSize {
					maxSize = s
				}
			}),
		)
	}

	if txn.Empty() {
		return 0, nil
	}

	if err := txn.Do(ctx).Err(); err != nil {
		return 0, err
	}

	return maxSize, nil
}
