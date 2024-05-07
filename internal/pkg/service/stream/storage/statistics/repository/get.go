package repository

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/aggregate"
)

// Provider is a unified interface to aggregate statistics directly from the database or from a cache.
type Provider interface {
	ProjectStats(ctx context.Context, k keboola.ProjectID) (statistics.Aggregated, error)
	BranchStats(ctx context.Context, k key.BranchKey) (statistics.Aggregated, error)
	SourceStats(ctx context.Context, k key.SourceKey) (statistics.Aggregated, error)
	SinkStats(ctx context.Context, k key.SinkKey) (statistics.Aggregated, error)
	FileStats(ctx context.Context, k model.FileKey) (statistics.Aggregated, error)
	SliceStats(ctx context.Context, k model.SliceKey) (statistics.Aggregated, error)
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

func (v *provider) FileStats(ctx context.Context, k model.FileKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

func (v *provider) SliceStats(ctx context.Context, k model.SliceKey) (statistics.Aggregated, error) {
	return v.fn(ctx, k)
}

// AggregateIn statistics from the database.
func (r *Repository) AggregateIn(objectKey fmt.Stringer) *op.TxnOp[statistics.Aggregated] {
	var result statistics.Aggregated
	txn := op.TxnWithResult(r.client, &result)
	for _, l := range level.AllLevels() {
		l := l

		// Get stats prefix for the slice state
		pfx := r.schema.InLevel(l).InObject(objectKey)

		// Sum
		txn.Then(pfx.GetAll(r.client).ForEach(func(v statistics.Value, header *iterator.Header) error {
			aggregate.Aggregate(l, v, &result)
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
