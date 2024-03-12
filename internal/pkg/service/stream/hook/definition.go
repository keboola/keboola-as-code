package hook

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

type sourceHook = func(now time.Time, sources *[]definition.Source, atomicOp *op.AtomicOpCore)

type sinkHook = func(now time.Time, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore)

func (r *Registry) OnSourceModification(fn sourceHook) {
	r.source = append(r.source, fn)
}

func (r *Registry) OnSinkModification(fn sinkHook) {
	r.sink = append(r.sink, fn)
}

func (e *Executor) OnSourceSave(now time.Time, sources *[]definition.Source, atomicOp *op.AtomicOpCore) {
	e.hooks.source.forEach(func(fn sourceHook) {
		fn(now, sources, atomicOp)
	})
}

func (e *Executor) OnSinkSave(now time.Time, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore) {
	e.hooks.sink.forEach(func(fn sinkHook) {
		fn(now, sinks, atomicOp)
	})
}
