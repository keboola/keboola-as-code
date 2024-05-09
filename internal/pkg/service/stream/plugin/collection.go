package plugin

import (
	"context"
	"reflect"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Collection struct {
	onBranchSave fnList[onBranchSaveFn]
	onSourceSave fnList[onSourceSaveFn]
	onSinkSave   fnList[onSinkSaveFn]
	onFileOpen   fnList[onFileOpenFn]
	onFileSave   fnList[onFileSaveFn]
	onSliceOpen  fnList[onSliceOpenFn]
	onSliceSave  fnList[onSliceSaveFn]
}

type onBranchSaveFn func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Branch)

type onSourceSaveFn func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Source)

type onSinkSaveFn func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink)

type onFileOpenFn func(ctx context.Context, now time.Time, sink definition.Sink, file *storage.File)

type onFileSaveFn func(ctx context.Context, now time.Time, original, updated *storage.File)

type onSliceOpenFn func(ctx context.Context, now time.Time, file storage.File, slice *storage.Slice)

type onSliceSaveFn func(ctx context.Context, now time.Time, original, updated *storage.Slice)

func (c *Collection) OnBranchSave(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, fn)
}

func (c *Collection) OnBranchDelete(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) {
		if isDelete(now, branch) {
			fn(ctx, now, by, original, branch)
		}
	})
}

func (c *Collection) OnBranchUndelete(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) {
		if isUndelete(now, branch) {
			fn(ctx, now, by, original, branch)
		}
	})
}

func (c *Collection) OnSourceSave(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, fn)
}

func (c *Collection) OnSourceDelete(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) {
		if isDelete(now, branch) {
			fn(ctx, now, by, original, branch)
		}
	})
}

func (c *Collection) OnSourceUndelete(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) {
		if isUndelete(now, branch) {
			fn(ctx, now, by, original, branch)
		}
	})
}

func (c *Collection) OnSinkSave(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, fn)
}

func (c *Collection) OnSinkActivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) {
		if isActivation(now, original, updated) {
			fn(ctx, now, by, original, updated)
		}
	})
}

func (c *Collection) OnSinkDeactivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) {
		if isDeactivation(now, updated) {
			fn(ctx, now, by, original, updated)
		}
	})
}

func (c *Collection) OnSinkModification(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
		if !isActivation(now, old, updated) && !isDeactivation(now, updated) && !reflect.DeepEqual(old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnFileOpen(fn onFileOpenFn) {
	c.onFileOpen = append(c.onFileOpen, fn)
}

func (c *Collection) OnFileSave(fn onFileSaveFn) {
	c.onFileSave = append(c.onFileSave, fn)
}

func (c *Collection) OnSliceOpen(fn onSliceOpenFn) {
	c.onSliceOpen = append(c.onSliceOpen, fn)
}

func (c *Collection) OnSliceSave(fn onSliceSaveFn) {
	c.onSliceSave = append(c.onSliceSave, fn)
}

func isDelete(now time.Time, updated definition.SoftDeletableInterface) bool {
	return updated.DeletedAt().Time().Equal(now)
}

func isUndelete(now time.Time, updated definition.SoftDeletableInterface) bool {
	return updated.UndeletedAt().Time().Equal(now)
}

func isActivation(now time.Time, old, updated definition.SwitchableInterface) bool {
	created := old == nil

	var undeleted bool
	if v, ok := updated.(definition.SoftDeletableInterface); ok {
		undeleted = isUndelete(now, v)
	}

	var enabled bool
	{
		at := updated.EnabledAt()
		enabled = at != nil && at.Time().Equal(now)
	}

	return created || undeleted || enabled
}

func isDeactivation(now time.Time, updated definition.SwitchableInterface) bool {
	var deleted bool
	if v, ok := updated.(definition.SoftDeletableInterface); ok {
		deleted = isDelete(now, v)
	}

	var disabled bool
	{
		at := updated.DisabledAt()
		disabled = at != nil && at.Time().Equal(now)
	}

	return deleted || disabled
}
