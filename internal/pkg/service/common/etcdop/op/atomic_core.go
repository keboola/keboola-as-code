package op

import (
	"context"
	"slices"

	etcd "go.etcd.io/etcd/client/v3"

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
	// processorFactory allows injection of the type-specific processors from the AtomicOp[R].
	processorFactory atomicOpCoreProcessorFactory
}

// atomicOpCoreProcessorFactory - aux struct to connect type specific processors from AtomicOp to AtomicOpCore.
type atomicOpCoreProcessorFactory func() func(ctx context.Context, r *TxnResult[NoResult])

// SkipPrefixKeysCheck disables the feature described bellow.
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

// ReadPhaseOps returns all op factories for READ phase,
// is used in joining two atomic operations, see AddFrom method.
func (v *AtomicOpCore) ReadPhaseOps() (out []HighLevelFactory) {
	return slices.Clone(v.readPhase)
}

// WritePhaseOps returns all op factories for WRITE phase,
// is used in joining two atomic operations, see AddFrom method.
func (v *AtomicOpCore) WritePhaseOps() (out []HighLevelFactory) {
	// If there is no processor callback and no lock (IF condition),
	// we can return operations without wrapping them to a TXN.
	if v.processorFactory == nil && len(v.locks) == 0 {
		return slices.Clone(v.writePhase)
	}

	return []HighLevelFactory{func(ctx context.Context) Op {
		if txn, err := v.writeTxn(ctx); err == nil {
			return txn
		} else {
			return ErrorOp(err)
		}
	}}
}

// Core - to match AtomicOpInterface.
func (v *AtomicOpCore) Core() *AtomicOpCore {
	return v
}

func (v *AtomicOpCore) Empty() bool {
	return len(v.locks) == 0 && len(v.readPhase) == 0 && len(v.writePhase) == 0
}

// AddFrom merges operations from some other atomic operation.
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

func (v *AtomicOpCore) newEmpty() *AtomicOpCore {
	out := newAtomicCore(v.client)
	out.checkPrefixKey = v.checkPrefixKey
	return out
}

func (v *AtomicOpCore) setProcessorFactory(fn atomicOpCoreProcessorFactory) {
	v.processorFactory = fn
}

func (v *AtomicOpCore) locksIfs() (cmps []etcd.Cmp, err error) {
	for _, lock := range v.locks {
		if !lock.IsLocked() {
			return nil, NotLockedError{}
		}
		cmps = append(cmps, lock.IsOwner())
	}
	return cmps, nil
}

func (v *AtomicOpCore) readTxn(ctx context.Context, tracker *TrackerKV) (*TxnOp[NoResult], error) {
	// Create READ transaction
	readTxn := Txn(tracker)
	for _, opFactory := range v.readPhase {
		op := opFactory(ctx)
		if errOp, ok := op.(*errorOp); ok {
			return nil, errOp.err
		} else if op != nil {
			readTxn.Merge(op)
		}
	}

	// Stop the READ phase, if a lock is not locked
	if lockIfs, err := v.locksIfs(); err == nil && len(lockIfs) > 0 {
		readTxn.Merge(Txn(nil).
			If(lockIfs...).
			OnFailed(func(r *TxnResult[NoResult]) {
				r.AddErr(errors.PrefixError(LockedError{}, "read phase"))
			}),
		)
	} else if err != nil {
		return nil, errors.PrefixError(err, "read phase")
	}

	return readTxn, nil
}

func (v *AtomicOpCore) writeTxn(ctx context.Context) (*TxnOp[NoResult], error) {
	// Create WRITE transaction
	writeTxn := Txn(v.client)
	for _, opFactory := range v.writePhase {
		op := opFactory(ctx)
		if errOp, ok := op.(*errorOp); ok {
			return nil, errOp.err
		} else if op != nil {
			writeTxn.Merge(op)
		}
	}

	// Stop the WRITE phase, if a lock is not locked
	if lockIfs, err := v.locksIfs(); err == nil && len(lockIfs) > 0 {
		writeTxn.Merge(Txn(nil).
			If(lockIfs...).
			OnFailed(func(r *TxnResult[NoResult]) {
				r.AddErr(errors.PrefixError(LockedError{}, "write phase"))
			}),
		)
	} else if err != nil {
		return nil, errors.PrefixError(err, "write phase")
	}

	// Attach processor callback, if any
	var processor func(ctx context.Context, r *TxnResult[NoResult])
	if v.processorFactory != nil {
		processor = v.processorFactory()
	}
	if processor != nil {
		writeTxn.AddProcessor(processor)
	}

	return writeTxn, nil
}

func (v *AtomicOpCore) do(ctx context.Context, tracker *TrackerKV, opts ...Option) (writeTxn *TxnOp[NoResult], readRevision int64, err error) {
	// Create READ phase
	readTxn, err := v.readTxn(ctx, tracker)
	if err != nil {
		return nil, 0, err
	}

	// Run READ phase, track used keys/prefixes
	if !readTxn.Empty() {
		if readResult := readTxn.Do(ctx, opts...); readResult.Err() == nil {
			readRevision = readResult.Header().Revision
		} else {
			return nil, 0, readResult.Err()
		}
	}

	// Create WRITE transaction
	writeTxn, err = v.writeTxn(ctx)
	if err != nil {
		return nil, 0, err
	}

	return writeTxn, readRevision, nil
}
