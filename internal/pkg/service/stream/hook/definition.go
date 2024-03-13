package hook

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

type sourceHook = func(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sources *[]definition.Source, atomicOp *op.AtomicOpCore)

type sinkHook = func(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore)

// OnSourceSave - see definition/repository.Hooks.
func (r *Registry) OnSourceSave(fn sourceHook) {
	r.source = append(r.source, fn)
}

// OnSinkSave - see definition/repository.Hooks.
func (r *Registry) OnSinkSave(fn sinkHook) {
	r.sink = append(r.sink, fn)
}

// OnSourceSave - see definition/repository.Hooks.
func (e *Executor) OnSourceSave(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sources *[]definition.Source, atomicOp *op.AtomicOpCore) {
	e.hooks.source.forEach(func(fn sourceHook) {
		fn(rb, now, parentKey, sources, atomicOp)
	})
}

// OnSinkSave - see definition/repository.Hooks.
func (e *Executor) OnSinkSave(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore) {
	e.hooks.sink.forEach(func(fn sinkHook) {
		fn(rb, now, parentKey, sinks, atomicOp)
	})
}
