package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type Hooks interface {
	// OnFileStateTransition is called on a file state transition.
	// The hook can perform related operations in other parts of the system.
	// Provided op.AtomicOp can be modified by the hook.
	OnFileStateTransition(fileKey model.FileKey, from, to model.FileState, atomicOp *op.AtomicOp[model.File])
	// OnSliceStateTransition is called on a slice state transition.
	// The hook can perform related operations in other parts of the system.
	// Provided op.AtomicOp can be modified by the hook.
	OnSliceStateTransition(sliceKey model.SliceKey, from, to model.SliceState, atomicOp *op.AtomicOp[model.Slice])
	// OnFileDelete is called on a file deletion during cleanup.
	// The hook can perform related operations in other parts of the system.
	// Provided op.AtomicOp can be modified by the hook.
	OnFileDelete(fileKey model.FileKey, atomicOp *op.AtomicOp[op.NoResult])
	// OnSliceDelete is called on a slice deletion during cleanup.
	// The hook can perform related operations in other parts of the system.
	// Provided op.AtomicOp can be modified by the hook.
	OnSliceDelete(sliceKey model.SliceKey, atomicOp *op.AtomicOp[op.NoResult])
}
