package repository

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

type Hooks interface {
	// OnSourceSave is called on all Source operations: create, update, rollback, soft-delete, undelete.
	// The provided op.AtomicOpCore can be modified.
	// The parentKey is parent of all modified Sources, for example BranchKey, it may be also SourceKey, if only one Source is modified.
	// The sources slices is filled in during the read phase, and is available in the write phase.
	OnSourceSave(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sources *[]definition.Source, atomicOp *op.AtomicOpCore)
	// OnSinkSave is called on all Sink operations: create, update, rollback, soft-delete, undelete.
	// The provided op.AtomicOpCore can be modified.
	// The parentKey is parent of all modified Sinks, for example SourceKey, it may be also SinkKey, if only one Sink is modified.
	// The sources sinks is filled in during the read phase, and is available in the write phase.
	OnSinkSave(rb rollback.Builder, now time.Time, parentKey fmt.Stringer, sinks *[]definition.Sink, atomicOp *op.AtomicOpCore)
}
