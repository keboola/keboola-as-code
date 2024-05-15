package op

import (
	"context"
	etcd "go.etcd.io/etcd/client/v3"
	"slices"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AtomicOpCore provides a common interface of the atomic operation, without result type specific methods.
// See the AtomicOp.Core method for details.
type AtomicOpCore struct {
	client         etcd.KV
	checkPrefixKey bool // checkPrefixKey - see SkipPrefixKeysCheck method documentation
	locks          []Mutex
	readPhase      []HighLevelFactory
	writePhase     []HighLevelFactory

// SkipPrefixKeysCheck disables the feature.
//
// By default, the feature is enabled and checks that each loaded key within the Read Phase, from a prefix, exists in Write Phase.
// This can be potentially SLOW and generate a lot of IF conditions, if there are a large number of keys in the prefix.
// Therefore, this feature can be turned off by the method.
//
// Modification of a key in the prefix is always detected,
// this feature is used to detect the deletion of a key from the prefix.
//
// See TestAtomicOp:GetPrefix_DeleteKey_SkipPrefixKeysCheck.
func (v *AtomicOpCore) SkipPrefixKeysCheck() *AtomicOpCore {
	v.checkPrefixKey = false
	return v
}

func (v *AtomicOpCore) AddFrom(ops ...AtomicOpInterface) *AtomicOpCore {
	for _, op := range ops {
		v.readPhase = append(v.readPhase, op.ReadPhaseOps()...)
		v.writePhase = append(v.writePhase, op.WritePhaseOps()...)
	}
	return v
}

func (v *AtomicOpCore) RequireLock(lock Mutex) *AtomicOpCore {
	v.locks = append(v.locks, lock)
	return v
}

// Read adds operations factories to the READ phase.
//
// The factory can return <nil>, if you want to execute some code during the READ phase,
// but no etcd operation is generated.
//
// The factory can return op.ErrorOp(err) OR op.ErrorTxn[T](err) to signal a static error.
func (v *AtomicOpCore) Read(factories ...HighLevelFactory) *AtomicOpCore {
	v.readPhase = append(v.readPhase, factories...)
	return v
}

// Write adds operations factories to the WRITE phase.
//
// The factory can return <nil>, if you want to execute some code during the READ phase,
// but no etcd operation is generated.
//
// The factory can return op.ErrorOp(err) OR op.ErrorTxn[T](err) to signal a static error.
func (v *AtomicOpCore) Write(factories ...HighLevelFactory) *AtomicOpCore {
	v.writePhase = append(v.writePhase, factories...)
	return v
}
