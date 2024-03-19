package plugin

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Executor struct {
	collection *Collection
}

func (e *Executor) OnBranchSave(ctx *SaveContext, v *definition.Branch) {
	e.collection.onBranchSave.forEach(func(fn onBranchSaveFn) {
		fn(ctx, v)
	})
}

func (e *Executor) OnSourceSave(ctx *SaveContext, v *definition.Source) {
	e.collection.onSourceSave.forEach(func(fn onSourceSaveFn) {
		fn(ctx, v)
	})
}
func (e *Executor) OnSinkSave(ctx *SaveContext, v *definition.Sink) {
	e.collection.onSinkSave.forEach(func(fn onSinkSaveFn) {
		fn(ctx, v)
	})
}

func (e *Executor) OnFileSave(ctx *SaveContext, v *storage.File) {
	e.collection.onFileSave.forEach(func(fn onFileSaveFn) {
		fn(ctx, v)
	})
}

func (e *Executor) OnSliceSave(ctx *SaveContext, v *storage.Slice) {
	e.collection.onSliceSave.forEach(func(fn onSliceSaveFn) {
		fn(ctx, v)
	})
}
