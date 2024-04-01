package plugin

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Executor struct {
	collection *Collection
}

func (e *Executor) OnBranchSave(ctx context.Context, old, updated *definition.Branch) {
	e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) {
		fn(ctx, old, updated)
	})
}

func (e *Executor) OnSourceSave(ctx context.Context, old, updated *definition.Source) {
	e.collection.onSourceSave.forEach(func(fn onSourceSaveFn) {
		fn(ctx, old, updated)
	})
}

func (e *Executor) OnSinkSave(ctx context.Context, old, updated *definition.Sink) {
	e.collection.onSinkSave.forEach(func(fn onSinkSaveFn) {
		fn(ctx, old, updated)
	})
}

func (e *Executor) OnFileSave(ctx context.Context, old, updated *storage.File) {
	e.collection.onFileSave.forEach(func(fn onFileSaveFn) {
		fn(ctx, old, updated)
	})
}

func (e *Executor) OnSliceSave(ctx context.Context, old, updated *storage.Slice) {
	e.collection.onSliceSave.forEach(func(fn onSliceSaveFn) {
		fn(ctx, old, updated)
	})
}
