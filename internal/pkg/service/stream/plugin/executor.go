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
	ctx = context.WithValue(ctx, UpdatedBranch, updated)
	e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) {
		fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSourceSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) {
	ctx = context.WithValue(ctx, UpdatedSource, updated)
	e.collection.onSourceSave.forEach(func(fn onSourceSaveFn) {
		fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSinkSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
	ctx = context.WithValue(ctx, UpdatedSink, updated)
	e.collection.onSinkSave.forEach(func(fn onSinkSaveFn) {
		fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnFileOpen(ctx context.Context, now time.Time, sink definition.Sink, file *storage.File) {
	ctx = context.WithValue(ctx, UpdatedFile, file)
	e.collection.onFileOpen.forEach(func(fn onFileOpenFn) {
		fn(ctx, now, sink, file)
	})
}

func (e *Executor) OnFileSave(ctx context.Context, now time.Time, original, updated *storage.File) {
	ctx = context.WithValue(ctx, UpdatedFile, updated)
	e.collection.onFileSave.forEach(func(fn onFileSaveFn) {
		fn(ctx, now, original, updated)
	})
}

func (e *Executor) OnSliceOpen(ctx context.Context, now time.Time, file storage.File, slice *storage.Slice) {
	ctx = context.WithValue(ctx, UpdatedSlice, slice)
	e.collection.onSliceOpen.forEach(func(fn onSliceOpenFn) {
		fn(ctx, now, file, slice)
	})
}

func (e *Executor) OnSliceSave(ctx context.Context, now time.Time, old, updated *storage.Slice) {
	ctx = context.WithValue(ctx, UpdatedSlice, updated)
	e.collection.onSliceSave.forEach(func(fn onSliceSaveFn) {
		fn(ctx, now, old, updated)
	})
}
