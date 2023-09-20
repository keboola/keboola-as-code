package repository

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type Provider interface {
	ReceiverStats(ctx context.Context, k key.ReceiverKey) (Aggregated, error)
	ExportStats(ctx context.Context, k key.ExportKey) (Aggregated, error)
	FileStats(ctx context.Context, k storage.FileKey) (Aggregated, error)
	SliceStats(ctx context.Context, k storage.SliceKey) (Aggregated, error)
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

func (v *getters) FileStats(ctx context.Context, k storage.FileKey) (Aggregated, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) SliceStats(ctx context.Context, k storage.SliceKey) (Aggregated, error) {
	return v.getStatsFn(ctx, k)
}
