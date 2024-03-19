package plugin

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Collection struct {
	executor     *Executor
	onBranchSave fnList[onBranchSaveFn]
	onSourceSave fnList[onSourceSaveFn]
	onSinkSave   fnList[onSinkSaveFn]
	onFileSave   fnList[onFileSaveFn]
	onSliceSave  fnList[onSliceSaveFn]
}

type onBranchSaveFn func(ctx *SaveContext, v *definition.Branch)

type onSourceSaveFn func(ctx *SaveContext, v *definition.Source)

type onSinkSaveFn func(ctx *SaveContext, v *definition.Sink)

type onFileSaveFn func(ctx *SaveContext, v *storage.File)

type onSliceSaveFn func(ctx *SaveContext, v *storage.Slice)

func (c *Collection) OnBranchSave(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, fn)
}

func (c *Collection) OnSourceSave(fn onSourceSaveFn) {
	c.onSourceSave = append(c.onSourceSave, fn)
}

func (c *Collection) OnSinkSave(fn onSinkSaveFn) {
	c.onSinkSave = append(c.onSinkSave, fn)
}

func (c *Collection) OnFileSave(fn onFileSaveFn) {
	c.onFileSave = append(c.onFileSave, fn)
}

func (c *Collection) OnSliceSave(fn onSliceSaveFn) {
	c.onSliceSave = append(c.onSliceSave, fn)
}
