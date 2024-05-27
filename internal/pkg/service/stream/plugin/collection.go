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

type onBranchSaveFn func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Branch) error

type onSourceSaveFn func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Source) error

type onSinkSaveFn func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) error

type onFileOpenFn func(ctx context.Context, now time.Time, sink definition.Sink, file *storage.File) error

type onFileSaveFn func(ctx context.Context, now time.Time, original, updated *storage.File) error

type onSliceOpenFn func(ctx context.Context, now time.Time, file storage.File, slice *storage.Slice) error

type onSliceSaveFn func(ctx context.Context, now time.Time, original, updated *storage.Slice) error

func (c *Collection) OnBranchSave(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, fn)
}

func (c *Collection) OnBranchEnable(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) error {
		if isEnabledNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnBranchDisable(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) error {
		if isDisabledNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnBranchActivation(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) error {
		if isActivatedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnBranchDeactivation(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) error {
		if isDeactivatedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnBranchDelete(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) error {
		if isDeletedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnBranchUndelete(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Branch) error {
		if isUndeletedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSourceSave(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, fn)
}

func (c *Collection) OnSourceEnabled(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) error {
		if isEnabledNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSourceDisabled(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) error {
		if isDisabledNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSourceActivation(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) error {
		if isActivatedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSourceDeactivation(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) error {
		if isDeactivatedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSourceModification(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Source) error {
		if !isActivatedNow(original, updated) && !isDeactivatedNow(original, updated) && !reflect.DeepEqual(original, updated) {
			return fn(ctx, now, by, original, updated)
		}
		return nil
	})
}

func (c *Collection) OnSourceDelete(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) error {
		if isDeletedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSourceUndelete(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, original, branch *definition.Source) error {
		if isUndeletedNow(original, branch) {
			return fn(ctx, now, by, original, branch)
		}
		return nil
	})
}

func (c *Collection) OnSinkSave(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, fn)
}

func (c *Collection) OnSinkEnabled(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) error {
		if isEnabledNow(original, updated) {
			return fn(ctx, now, by, original, updated)
		}
		return nil
	})
}

func (c *Collection) OnSinkDisabled(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) error {
		if isDisabledNow(original, updated) {
			return fn(ctx, now, by, original, updated)
		}
		return nil
	})
}

func (c *Collection) OnSinkActivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) error {
		if isActivatedNow(original, updated) {
			return fn(ctx, now, by, original, updated)
		}
		return nil
	})
}

func (c *Collection) OnSinkDeactivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) error {
		if isDeactivatedNow(original, updated) {
			return fn(ctx, now, by, original, updated)
		}
		return nil
	})
}

func (c *Collection) OnSinkModification(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Sink) error {
		if !isActivatedNow(original, updated) && !isDeactivatedNow(original, updated) && !reflect.DeepEqual(original, updated) {
			return fn(ctx, now, by, original, updated)
		}
		return nil
	})
}

func (c *Collection) OnFileOpen(fn onFileOpenFn) {
	c.onFileOpen = append(c.onFileOpen, fn)
}

func (c *Collection) OnFileClose(fn onFileSaveFn) {
	c.OnFileSave(func(ctx context.Context, now time.Time, original, file *storage.File) error {
		if original != nil && original.State != file.State && file.State == storage.FileClosing {
			return fn(ctx, now, original, file)
		}
		return nil
	})
}

func (c *Collection) OnFileUpdate(fn onFileSaveFn) {
	c.OnFileSave(func(ctx context.Context, now time.Time, original, file *storage.File) error {
		if original != nil && !file.Deleted && !reflect.DeepEqual(original, file) {
			return fn(ctx, now, original, file)
		}
		return nil
	})
}

func (c *Collection) OnFileDelete(fn onFileSaveFn) {
	c.OnFileSave(func(ctx context.Context, now time.Time, original, file *storage.File) error {
		if file.Deleted {
			return fn(ctx, now, original, file)
		}
		return nil
	})
}

func (c *Collection) OnFileSave(fn onFileSaveFn) {
	c.onFileSave = append(c.onFileSave, fn)
}

func (c *Collection) OnSliceOpen(fn onSliceOpenFn) {
	c.onSliceOpen = append(c.onSliceOpen, fn)
}

func (c *Collection) OnSliceClose(fn onSliceSaveFn) {
	c.OnSliceSave(func(ctx context.Context, now time.Time, original, slice *storage.Slice) error {
		if original != nil && original.State != slice.State && slice.State == storage.SliceClosing {
			return fn(ctx, now, original, slice)
		}
		return nil
	})
}

func (c *Collection) OnSliceUpdate(fn onSliceSaveFn) {
	c.OnSliceSave(func(ctx context.Context, now time.Time, original, slice *storage.Slice) error {
		if original != nil && !slice.Deleted && !reflect.DeepEqual(original, slice) {
			return fn(ctx, now, original, slice)
		}
		return nil
	})
}

func (c *Collection) OnSliceDelete(fn onSliceSaveFn) {
	c.OnSliceSave(func(ctx context.Context, now time.Time, original, slice *storage.Slice) error {
		if slice.Deleted {
			return fn(ctx, now, original, slice)
		}
		return nil
	})
}

func (c *Collection) OnSliceSave(fn onSliceSaveFn) {
	c.onSliceSave = append(c.onSliceSave, fn)
}
