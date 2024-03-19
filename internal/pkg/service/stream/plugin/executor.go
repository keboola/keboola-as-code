package plugin

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"

type Executor struct {
	collection *Collection
}

func (e *Executor) OnBranchSave(ctx *SaveContext, v *definition.Branch) {
	e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) {
		fn(ctx, v)
	})
}

func (e *Executor) OnSourceSave(ctx *SaveContext, v *definition.Source) {
	e.collection.onSourceSaveFn.forEach(func(fn onSourceSaveFn) {
		fn(ctx, v)
	})
}
func (e *Executor) OnSinkSave(ctx *SaveContext, v *definition.Sink) {
	e.collection.onSinkSaveFn.forEach(func(fn onSinkSaveFn) {
		fn(ctx, v)
	})
}
