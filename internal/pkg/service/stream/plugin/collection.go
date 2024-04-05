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
	onFileSave   fnList[onFileSaveFn]
	onSliceSave  fnList[onSliceSaveFn]
}

type onBranchSaveFn func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch)

type onSourceSaveFn func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source)

type onSinkSaveFn func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink)

type onFileSaveFn func(ctx context.Context, now time.Time, old, updated *storage.File)

type onSliceSaveFn func(ctx context.Context, now time.Time, old, updated *storage.Slice)

func (c *Collection) OnBranchSave(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, fn)
}

func (c *Collection) OnBranchDelete(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) {
		if isDelete(now, old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnBranchUndelete(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) {
		if isUndelete(now, old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnSourceSave(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, fn)
}

func (c *Collection) OnSourceDelete(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) {
		if isDelete(now, old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnSourceUndelete(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) {
		if isUndelete(now, old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnSinkSave(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, fn)
}

func (c *Collection) OnSinkActivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
		if isActivation(now, old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnSinkDeactivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
		if isDeactivation(now, old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnSinkModification(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) {
		if !isActivation(now, old, updated) && !isDeactivation(now, old, updated) && !reflect.DeepEqual(old, updated) {
			fn(ctx, now, by, old, updated)
		}
	})
}

func (c *Collection) OnFileSave(fn onFileSaveFn) {
	c.onFileSave = append(c.onFileSave, fn)
}

func (c *Collection) OnSliceSave(fn onSliceSaveFn) {
	c.onSliceSave = append(c.onSliceSave, fn)
}

func isDelete(now time.Time, old, updated definition.SoftDeletableInterface) bool {
	at := updated.EntityDeletedAt()
	return at != nil && at.Time().Equal(now)
}

func isUndelete(now time.Time, old, updated definition.SoftDeletableInterface) bool {
	at := updated.EntityUndeletedAt()
	return at != nil && at.Time().Equal(now)
}

func isActivation(now time.Time, old, updated definition.SwitchableInterface) bool {
	created := old == nil

	undeleted := false
	if v, ok := updated.(definition.SoftDeletableInterface); ok {
		undeleted = isUndelete(now, old.(definition.SoftDeletableInterface), v)
	}

	enabled := false
	{
		at := updated.EntityEnabledAt()
		enabled = at != nil && at.Time().Equal(now)
	}

	return created || undeleted || enabled
}

func isDeactivation(now time.Time, old, updated definition.SwitchableInterface) bool {
	deleted := false
	if v, ok := updated.(definition.SoftDeletableInterface); ok {
		deleted = isDelete(now, old.(definition.SoftDeletableInterface), v)
	}

	disabled := false
	{
		at := updated.EntityDisabledAt()
		disabled = at != nil && at.Time().Equal(now)
	}

	return deleted || disabled
}
