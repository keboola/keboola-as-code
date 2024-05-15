package op

import (
	etcd "go.etcd.io/etcd/client/v3"
)

// AtomicOpCore provides a common interface of the atomic operation, without result type specific methods.
// See the AtomicOp.Core method for details.
type AtomicOpCore struct {
	client     etcd.KV
	readPhase  []HighLevelFactory
	writePhase []HighLevelFactory
	locks          []Mutex
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
