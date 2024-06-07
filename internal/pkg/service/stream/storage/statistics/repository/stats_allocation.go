package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) estimateSliceSizeOnSliceOpen() {
	r.plugins.Collection().OnSliceOpen(func(ctx context.Context, now time.Time, file model.File, slice *model.Slice) error {
		// Optional: If Sink is new, we can skip calculation, there is no previous slice/statistics
		if sink := plugin.SinkFromContext(ctx, file.SinkKey); sink != nil && sink.CreatedAt().Time().Equal(now) {
			slice.LocalStorage.AllocatedDiskSpace = file.LocalStorage.DiskAllocation.ForNextSlice(0)
			return nil
		}

		// The operation is performed immediately, outside the atomic operation!
		// We need to update the value to the Slice entity before saving.
		if err := r.estimateSliceSize(file, slice).Do(ctx).Err(); err != nil {
			// Error is not fatal
			r.logger.Errorf(ctx, `cannot calculate slice pre-allocated size: %s`, err)
		}
		return nil
	})
}

func (r *Repository) estimateSliceSize(file model.File, slice *model.Slice) *op.TxnOp[datasize.ByteSize] {
	return r.
		maxUsedDiskSizeBySliceIn(slice.SinkKey, recordsForSliceDiskSizeCalc).
		OnResult(func(r *op.TxnResult[datasize.ByteSize]) {
			slice.LocalStorage.AllocatedDiskSpace = file.LocalStorage.DiskAllocation.ForNextSlice(r.Result())
		})
}

// maxUsedDiskSizeBySliceIn scans the statistics in the parentKey, scanned are:
//   - The last <limit> slices in level.Staging (uploaded slices).
//   - The last <limit> slices in level.Target  (imported slices).
func (r *Repository) maxUsedDiskSizeBySliceIn(parentKey fmt.Stringer, limit int) *op.TxnOp[datasize.ByteSize] {
	var maxSize datasize.ByteSize
	txn := op.TxnWithResult(r.client, &maxSize)
	for _, l := range []level.Level{level.Staging, level.Target} {
		// Get maximum
		txn.Then(
			r.schema.
				InLevel(l).InObject(parentKey).
				GetAll(r.client, iterator.WithLimit(limit), iterator.WithSort(etcd.SortDescend)).
				ForEach(func(v statistics.Value, header *iterator.Header) error {
					// Ignore sums
					if v.SlicesCount == 1 && v.CompressedSize > maxSize {
						maxSize = v.CompressedSize
					}
					return nil
				}))
	}
	return txn
}
