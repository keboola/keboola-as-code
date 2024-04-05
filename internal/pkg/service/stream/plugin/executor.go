package plugin

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Executor struct {
	collection *Collection
}

func (e *Executor) OnBranchSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) {
	e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) {
		fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSourceSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) {
	e.collection.onSourceSave.forEach(func(fn onSourceSaveFn) {
		fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSinkSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
	e.collection.onSinkSave.forEach(func(fn onSinkSaveFn) {
		fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnFileSave(ctx context.Context, now time.Time, old, updated *storage.File) {
	e.collection.onFileSave.forEach(func(fn onFileSaveFn) {
		fn(ctx, now, old, updated)
	})
}

func (e *Executor) OnSliceSave(ctx context.Context, now time.Time, old, updated *storage.Slice) {
	e.collection.onSliceSave.forEach(func(fn onSliceSaveFn) {
		fn(ctx, now, old, updated)
	})
}
