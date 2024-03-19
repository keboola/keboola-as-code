package plugin

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"

type Collection struct {
	executor       *Executor
	onBranchSave   fnList[onBranchSaveFn]
	onSourceSaveFn fnList[onSourceSaveFn]
	onSinkSaveFn   fnList[onSinkSaveFn]
}

type onBranchSaveFn func(ctx *SaveContext, v *definition.Branch)

type onSourceSaveFn func(ctx *SaveContext, v *definition.Source)

type onSinkSaveFn func(ctx *SaveContext, v *definition.Sink)

func (c *Collection) OnBranchSave(fn onBranchSaveFn) {
	c.onBranchSave = append(c.onBranchSave, fn)
}

func (c *Collection) OnSourceSave(fn onSourceSaveFn) {
	c.onSourceSaveFn = append(c.onSourceSaveFn, fn)
}

func (c *Collection) OnSinkSave(fn onSinkSaveFn) {
	c.onSinkSaveFn = append(c.onSinkSaveFn, fn)
}
