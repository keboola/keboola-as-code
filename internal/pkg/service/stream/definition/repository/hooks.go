package repository

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

type Hooks interface {
	// OnSourceSave is called on the following Source operations: create, update, rollback, soft-delete, undelete.
	// The method in called during the read phase of the op.AtomicOp.
	// The provided write transaction can be modified by the hook, and it is invoked during the write phase of the op.AtomicOp.
	OnSourceSave(now time.Time, sources *[]definition.Source, atomicOp *op.AtomicOpCore)
	// OnSinkSave is called on the following Sink operations: create, update, rollback, soft-delete, undelete.
	// The method in called during the read phase of the op.AtomicOp.
	// The provided write transaction can be modified by the hook, and it is invoked during the write phase of the op.AtomicOp.
	OnSinkSave(now time.Time, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore)
}
