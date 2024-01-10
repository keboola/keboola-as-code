package repository

import (
	"context"
	"fmt"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/aggregate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

// Provider is a unified interface to aggregate statistics directly from the database or from a cache.
type Provider interface {
	ProjectStats(ctx context.Context, k keboola.ProjectID) (statistics.Aggregated, error)
	BranchStats(ctx context.Context, k key.BranchKey) (statistics.Aggregated, error)
	SourceStats(ctx context.Context, k key.SourceKey) (statistics.Aggregated, error)
	SinkStats(ctx context.Context, k key.SinkKey) (statistics.Aggregated, error)
	FileStats(ctx context.Context, k storage.FileKey) (statistics.Aggregated, error)
	SliceStats(ctx context.Context, k storage.SliceKey) (statistics.Aggregated, error)
}

// aggregateFn abstract the way to obtain aggregated statistics for the object key.
type aggregateFn func(ctx context.Context, objectKey fmt.Stringer) (statistics.Aggregated, error)

// provider implements the Provider interface.
// The way to get statistics is abstracted using aggregateFn,
// the provider struct adds only a nicer interface for the function.
type provider struct {
	fn aggregateFn
}

func NewProvider(fn aggregateFn) Provider {
	return &provider{fn: fn}
}

func (v *provider) ProjectStats(ctx context.Context, k keboola.ProjectID) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) BranchStats(ctx context.Context, k key.BranchKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) SourceStats(ctx context.Context, k key.SourceKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) SinkStats(ctx context.Context, k key.SinkKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) FileStats(ctx context.Context, k storage.FileKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) SliceStats(ctx context.Context, k storage.SliceKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

// MaxUsedDiskSizeBySliceIn scans the statistics in the parentKey, scanned are:
//   - The last <limit> slices in storage.LevelStaging (uploaded slices).
//   - The last <limit> slices in storage.LevelTarget (imported slices).
func (r *Repository) MaxUsedDiskSizeBySliceIn(parentKey fmt.Stringer, limit int) *op.TxnOp[datasize.ByteSize] {
	var maxSize datasize.ByteSize
	txn := op.TxnWithResult(r.client, &maxSize)
	for _, level := range []storage.Level{storage.LevelStaging, storage.LevelTarget} {
		// Get maximum
		txn.Then(
			r.schema.
				InLevel(level).InObject(parentKey).
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

// AggregateIn statistics from the database.
func (r *Repository) AggregateIn(objectKey fmt.Stringer) *op.TxnOp[statistics.Aggregated] {
	var result statistics.Aggregated
	txn := op.TxnWithResult(r.client, &result)
	for _, level := range storage.AllLevels() {
		level := level

		// Get stats prefix for the slice state
		pfx := r.schema.InLevel(level).InObject(objectKey)

		// Sum
		txn.Then(pfx.GetAll(r.client).ForEach(func(v statistics.Value, header *iterator.Header) error {
			aggregate.Aggregate(level, v, &result)
			return nil
		}))
	}
	return txn
}

// aggregate statistics from the database.
func (r *Repository) aggregate(ctx context.Context, objectKey fmt.Stringer) (out statistics.Aggregated, err error) {
	txn := r.AggregateIn(objectKey)
	return txn.Do(ctx).ResultOrErr()
}
