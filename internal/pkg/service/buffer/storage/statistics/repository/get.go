package repository

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics/aggregate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

// Provider is a unified interface to aggregate statistics directly from the database or from a cache.
type Provider interface {
	ProjectStats(ctx context.Context, k keboola.ProjectID) (statistics.Aggregated, error)
	ReceiverStats(ctx context.Context, k key.ReceiverKey) (statistics.Aggregated, error)
	ExportStats(ctx context.Context, k key.ExportKey) (statistics.Aggregated, error)
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

func (v *provider) ReceiverStats(ctx context.Context, k key.ReceiverKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) ExportStats(ctx context.Context, k key.ExportKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) FileStats(ctx context.Context, k storage.FileKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) SliceStats(ctx context.Context, k storage.SliceKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

// aggregate statistics from the database.
func (r *Repository) aggregate(ctx context.Context, objectKey fmt.Stringer) (out statistics.Aggregated, err error) {
	txn := op.NewTxnOp()
	for _, level := range storage.AllLevels() {
		level := level

		// Get stats prefix for the slice state
		pfx := r.schema.InLevel(level).InObject(objectKey)

		// Sum
		txn.Then(pfx.GetAll().ForEachOp(func(v statistics.Value, header *iterator.Header) error {
			aggregate.Aggregate(level, v, &out)
			return nil
		}))
	}

	// Get all values in a transaction
	if err := txn.DoOrErr(ctx, r.client); err != nil {
		return out, err
	}

	return out, nil
}
