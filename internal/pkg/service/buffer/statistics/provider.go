package statistics

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Provider interface {
	ReceiverStats(ctx context.Context, k key.ReceiverKey) (Aggregated, error)
	ExportStats(ctx context.Context, k key.ExportKey) (Aggregated, error)
	FileStats(ctx context.Context, k key.FileKey) (Aggregated, error)
	SliceStats(ctx context.Context, k key.SliceKey) (Aggregated, error)
}

func newGetters(fn getStatsFn) *getters {
	return &getters{getStatsFn: fn}
}

type getters struct {
	getStatsFn getStatsFn
}

type getStatsFn func(ctx context.Context, objectKey fmt.Stringer) (Aggregated, error)

func (v *getters) ReceiverStats(ctx context.Context, k key.ReceiverKey) (Aggregated, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) ExportStats(ctx context.Context, k key.ExportKey) (Aggregated, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) FileStats(ctx context.Context, k key.FileKey) (Aggregated, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) SliceStats(ctx context.Context, k key.SliceKey) (Aggregated, error) {
	return v.getStatsFn(ctx, k)
}

func aggregate(category Category, partial Value, out *Aggregated) {
	switch category {
	case Buffered:
		out.Buffered = out.Buffered.Add(partial)
		out.Total = out.Total.Add(partial)
	case Uploaded:
		out.Uploaded = out.Uploaded.Add(partial)
		out.Total = out.Total.Add(partial)
	case Imported:
		out.Imported = out.Imported.Add(partial)
		out.Total = out.Total.Add(partial)
	default:
		panic(errors.Errorf(`unexpected statistics category "%v"`, category))
	}
}
