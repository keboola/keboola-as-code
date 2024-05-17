package plugin

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Executor struct {
	logger     log.Logger
	collection *Collection
}

func (e *Executor) OnBranchSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) error {
	e.logger.Infof(ctx, `executing OnBranchSave "%s"`, updated.BranchKey.String())
	ctx = context.WithValue(ctx, updatedBranch, updated)
	return e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) error {
		return fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnSourceSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) error {
	e.logger.Infof(ctx, `executing OnSourceSave "%s"`, updated.SourceKey.String())
	ctx = context.WithValue(ctx, updatedSource, updated)
	return e.collection.onSourceSave.forEach(func(fn onSourceSaveFn) error {
		if err := fn(ctx, now, by, old, updated); err != nil {
			return err
		}
		return nil
	})
}

func (e *Executor) OnSinkSave(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) error {
	e.logger.Infof(ctx, `executing OnSinkSave "%s"`, updated.SinkKey.String())
	ctx = context.WithValue(ctx, updatedSink, updated)
	return e.collection.onSinkSave.forEach(func(fn onSinkSaveFn) error {
		return fn(ctx, now, by, old, updated)
	})
}

func (e *Executor) OnFileOpen(ctx context.Context, now time.Time, sink definition.Sink, file *storage.File) error {
	e.logger.Infof(ctx, `executing OnFileOpen "%s"`, file.SinkKey.String())
	ctx = context.WithValue(ctx, updatedFile, file)
	return e.collection.onFileOpen.forEach(func(fn onFileOpenFn) error {
		return fn(ctx, now, sink, file)
	})
}

func (e *Executor) OnFileSave(ctx context.Context, now time.Time, original, updated *storage.File) error {
	e.logger.Infof(ctx, `executing OnFileSave "%s"`, updated.FileKey.String())
	ctx = context.WithValue(ctx, updatedFile, updated)
	return e.collection.onFileSave.forEach(func(fn onFileSaveFn) error {
		return fn(ctx, now, original, updated)
	})
}

func (e *Executor) OnSliceOpen(ctx context.Context, now time.Time, file storage.File, slice *storage.Slice) error {
	e.logger.Infof(ctx, `executing OnSliceOpen "%s"`, slice.SliceKey.String())
	ctx = context.WithValue(ctx, updatedSlice, slice)
	return e.collection.onSliceOpen.forEach(func(fn onSliceOpenFn) error {
		return fn(ctx, now, file, slice)
	})
}

func (e *Executor) OnSliceSave(ctx context.Context, now time.Time, old, updated *storage.Slice) error {
	e.logger.Infof(ctx, `executing OnSliceSave "%s"`, updated.SliceKey.String())
	ctx = context.WithValue(ctx, updatedSlice, updated)
	return e.collection.onSliceSave.forEach(func(fn onSliceSaveFn) error {
		return fn(ctx, now, old, updated)
	})
}
