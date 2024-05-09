package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) estimateSliceSizeOnSliceCreate() {
	r.plugins.Collection().OnSliceOpen(func(ctx context.Context, now time.Time, file model.File, slice *model.Slice) {
		if err := r.estimateSliceSize(file, slice).Do(ctx).Err(); err != nil {
			// Error is not fatal
			r.logger.Errorf(ctx, `cannot calculate slice pre-allocated size: %s`, err)
		}
	})
}

func (r *Repository) estimateSliceSize(file model.File, slice *model.Slice) *op.AtomicOp[op.NoResult] {
	return op.Atomic(r.client, &op.NoResult{}).Read(func(ctx context.Context) op.Op {
		// Calculate pre-allocated size
		return r.
			maxUsedDiskSizeBySliceIn(slice.SinkKey, recordsForSliceDiskSizeCalc).
			OnResult(func(r *op.TxnResult[datasize.ByteSize]) {
				slice.LocalStorage.AllocatedDiskSpace = file.LocalStorage.DiskAllocation.ForNextSlice(r.Result())
			})
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
