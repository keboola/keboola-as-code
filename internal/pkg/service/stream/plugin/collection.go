package plugin

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"reflect"
)

type Collection struct {
	onBranchSave fnList[onBranchSaveFn]
	onSourceSave fnList[onSourceSaveFn]
	onSinkSave   fnList[onSinkSaveFn]
	onFileSave   fnList[onFileSaveFn]
	onSliceSave  fnList[onSliceSaveFn]
}

type onBranchSaveFn func(ctx context.Context, old, updated *definition.Branch)

type onSourceSaveFn func(ctx context.Context, old, updated *definition.Source)

type onSinkSaveFn func(ctx context.Context, old, updated *definition.Sink)

type onFileSaveFn func(ctx context.Context, old, updated *storage.File)

type onSliceSaveFn func(ctx context.Context, old, updated *storage.Slice)

func (c *Collection) OnBranchSave(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, fn)
}

func (c *Collection) OnSourceSave(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, fn)
}

func (c *Collection) OnSinkSave(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, fn)
}

func (c *Collection) OnSinkActivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, old, updated *definition.Sink) {
		if isSinkActivation(ctx, old, updated) {
			fn(ctx, old, updated)
		}
	})
}

func (c *Collection) OnSinkDeactivation(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, old, updated *definition.Sink) {
		if isSinkDeactivation(ctx, old, updated) {
			fn(ctx, old, updated)
		}
	})
}

func (c *Collection) OnSinkModification(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, func(ctx context.Context, old, updated *definition.Sink) {
		if !isSinkActivation(ctx, old, updated) && !isSinkDeactivation(ctx, old, updated) && !reflect.DeepEqual(old, updated) {
			fn(ctx, old, updated)
		}
	})
}

func (c *Collection) OnFileSave(fn onFileSaveFn) {
	c.onFileSave = append(c.onFileSave, fn)
}

func (c *Collection) OnSliceSave(fn onSliceSaveFn) {
	c.onSliceSave = append(c.onSliceSave, fn)
}

func isSinkActivation(ctx context.Context, old, updated *definition.Sink) bool {
	created := old == nil
	undeleted := updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(ctx.Now())
	enabled := updated.EnabledAt != nil && updated.EnabledAt.Time().Equal(ctx.Now())
	return created || undeleted || enabled
}

func isSinkDeactivation(ctx context.Context, old, updated *definition.Sink) bool {
	deleted := updated.DeletedAt != nil && updated.DeletedAt.Time().Equal(ctx.Now())
	disabled := updated.DisabledAt != nil && updated.DisabledAt.Time().Equal(ctx.Now())
	return deleted || disabled
}
