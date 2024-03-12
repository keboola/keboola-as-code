package hook

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type fileStateTransitionHook = func(fileKey model.FileKey, from, to model.FileState, atomicOp *op.AtomicOp[model.File])

type sliceStateTransitionHook = func(sliceKey model.SliceKey, from, to model.SliceState, atomicOp *op.AtomicOp[model.Slice])

type fileDeleteHook = func(fileKey model.FileKey, atomicOp *op.AtomicOp[op.NoResult])

type sliceDeleteHook = func(sliceKey model.SliceKey, atomicOp *op.AtomicOp[op.NoResult])

func (r *Registry) OnFileStateTransition(fn fileStateTransitionHook) {
	r.fileStateTransition = append(r.fileStateTransition, fn)
}

func (r *Registry) OnSliceStateTransition(fn sliceStateTransitionHook) {
	r.sliceStateTransition = append(r.sliceStateTransition, fn)
}

func (r *Registry) OnFileDelete(fn fileDeleteHook) {
	r.fileDelete = append(r.fileDelete, fn)
}

func (r *Registry) OnSliceDelete(fn sliceDeleteHook) {
	r.sliceDelete = append(r.sliceDelete, fn)
}

func (e *Executor) OnFileStateTransition(fileKey model.FileKey, from, to model.FileState, atomicOp *op.AtomicOp[model.File]) {
	e.hooks.fileStateTransition.forEach(func(fn fileStateTransitionHook) {
		fn(fileKey, from, to, atomicOp)
	})
}

func (e *Executor) OnSliceStateTransition(sliceKey model.SliceKey, from, to model.SliceState, atomicOp *op.AtomicOp[model.Slice]) {
	e.hooks.sliceStateTransition.forEach(func(fn sliceStateTransitionHook) {
		fn(sliceKey, from, to, atomicOp)
	})
}

func (e *Executor) OnFileDelete(fileKey model.FileKey, atomicOp *op.AtomicOp[op.NoResult]) {
	e.hooks.fileDelete.forEach(func(fn fileDeleteHook) {
		fn(fileKey, atomicOp)
	})
}

func (e *Executor) OnSliceDelete(sliceKey model.SliceKey, atomicOp *op.AtomicOp[op.NoResult]) {
	e.hooks.sliceDelete.forEach(func(fn sliceDeleteHook) {
		fn(sliceKey, atomicOp)
	})
}
