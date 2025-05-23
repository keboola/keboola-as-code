package plugin

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Executor struct {
	logger     log.Logger
	collection *Collection
}

func (e *Executor) OnBranchSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) error {
	e.logger.Debugf(ctx, `executing OnBranchSave "%s"`, updated.String())
	op.AtomicOpCtxFrom(ctx).SetValue(updated.BranchKey, updated)
	return e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) error {
		return fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSourceSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) error {
	e.logger.Debugf(ctx, `executing OnSourceSave "%s"`, updated.String())
	op.AtomicOpCtxFrom(ctx).SetValue(updated.SourceKey, updated)
	return e.collection.onSourceSave.forEach(func(fn onSourceSaveFn) error {
		return fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSinkSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) error {
	e.logger.Debugf(ctx, `executing OnSinkSave "%s"`, updated.String())
	op.AtomicOpCtxFrom(ctx).SetValue(updated.SinkKey, updated)
	return e.collection.onSinkSave.forEach(func(fn onSinkSaveFn) error {
		return fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnFileOpen(ctx context.Context, now time.Time, sink definition.Sink, file *storage.File) error {
	e.logger.Debugf(ctx, `executing OnFileOpen "%s"`, file.SinkKey.String())
	op.AtomicOpCtxFrom(ctx).SetValue(file.FileKey, file)
	return e.collection.onFileOpen.forEach(func(fn onFileOpenFn) error {
		return fn(ctx, now, sink, file)
	})
}

func (e *Executor) OnFileSave(ctx context.Context, now time.Time, original, updated *storage.File) error {
	e.logger.Debugf(ctx, `executing OnFileSave "%s"`, updated.String())
	op.AtomicOpCtxFrom(ctx).SetValue(updated.FileKey, updated)
	return e.collection.onFileSave.forEach(func(fn onFileSaveFn) error {
		return fn(ctx, now, original, updated)
	})
}

func (e *Executor) OnSliceOpen(ctx context.Context, now time.Time, file storage.File, slice *storage.Slice) error {
	e.logger.Debugf(ctx, `executing OnSliceOpen "%s"`, slice.String())
	op.AtomicOpCtxFrom(ctx).SetValue(slice.SliceKey, slice)
	return e.collection.onSliceOpen.forEach(func(fn onSliceOpenFn) error {
		return fn(ctx, now, file, slice)
	})
}

func (e *Executor) OnSliceSave(ctx context.Context, now time.Time, old, updated *storage.Slice) error {
	e.logger.Debugf(ctx, `executing OnSliceSave "%s"`, updated.String())
	op.AtomicOpCtxFrom(ctx).SetValue(updated.SliceKey, updated)
	return e.collection.onSliceSave.forEach(func(fn onSliceSaveFn) error {
		return fn(ctx, now, old, updated)
	})
}
