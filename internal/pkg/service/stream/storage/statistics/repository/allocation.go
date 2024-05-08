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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) estimateSliceSizeOnSliceCreate() {
	r.plugins.Collection().OnSliceSave(func(ctx context.Context, now time.Time, original, updated *model.Slice) {
		if original == nil {
			if err := r.estimateSliceSize(updated).Do(ctx).Err(); err != nil {
				// Error is not fatal
				r.logger.Errorf(ctx, `cannot calculate slice pre-allocated size: %s`, err)
			}
		}
	})
}

func (r *Repository) estimateSliceSize(slice *model.Slice) *op.AtomicOp[op.NoResult] {
	return op.Atomic(r.client, &op.NoResult{}).Read(func(ctx context.Context) op.Op {
		// Get disk allocation config
		cfg, ok := diskalloc.ConfigFromContext(ctx)
		if !ok {
			return nil
		}

		// Calculate pre-allocated size
		return r.
			maxUsedDiskSizeBySliceIn(slice.SinkKey, recordsForSliceDiskSizeCalc).
			OnResult(func(r *op.TxnResult[datasize.ByteSize]) {
				slice.LocalStorage.AllocatedDiskSpace = cfg.ForNextSlice(r.Result())
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
