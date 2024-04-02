package op

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"slices"

	etcd "go.etcd.io/etcd/client/v3"
)

// AtomicOpCore provides a common interface of the atomic operation, without result type specific methods.
// See the AtomicOp.Core method for details.
type AtomicOpCore struct {
	client           etcd.KV
	checkPrefixKey   bool // checkPrefixKey - see SkipPrefixKeysCheck method documentation
	locks            []Mutex
	readPhase        []HighLevelFactory
	writePhase       []HighLevelFactory
	processorFactory atomicOpCoreProcessorFactory
}

type atomicOpCoreProcessorFactory func() func(ctx context.Context, r *TxnResult[NoResult])

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

func (v *AtomicOpCore) ReadPhaseOps() (out []HighLevelFactory) {
	return slices.Clone(v.readPhase)
}

func (v *AtomicOpCore) WritePhaseOps() (out []HighLevelFactory) {
	if v.processorFactory == nil && len(v.locks) == 0 {
		return slices.Clone(v.writePhase)
	}

	return []HighLevelFactory{func(ctx context.Context) (Op, error) {
		return v.writeTxn(ctx)
	}}
}

func (v *AtomicOpCore) Core() *AtomicOpCore {
	return v
}

func (v *AtomicOpCore) Empty() bool {
	return len(v.locks) == 0 && len(v.readPhase) == 0 && len(v.writePhase) == 0
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

func (v *AtomicOpCore) ReadOp(ops ...Op) *AtomicOpCore {
	for _, op := range ops {
		v.Read(func(ctx context.Context) Op {
			return op
		})
	}
	return v
}

func (v *AtomicOpCore) Read(factories ...func(ctx context.Context) Op) *AtomicOpCore {
	for _, fn := range factories {
		v.ReadOrErr(func(ctx context.Context) (Op, error) {
			return fn(ctx), nil
		})
	}
	return v
}

func (v *AtomicOpCore) OnRead(fns ...func(ctx context.Context)) *AtomicOpCore {
	for _, fn := range fns {
		v.ReadOrErr(func(ctx context.Context) (Op, error) {
			fn(ctx)
			return nil, nil
		})
	}
	return v
}

func (v *AtomicOpCore) OnReadOrErr(fns ...func(ctx context.Context) error) *AtomicOpCore {
	for _, fn := range fns {
		v.ReadOrErr(func(ctx context.Context) (Op, error) {
			return nil, fn(ctx)
		})
	}
	return v
}

func (v *AtomicOpCore) ReadOrErr(factories ...HighLevelFactory) *AtomicOpCore {
	v.readPhase = append(v.readPhase, factories...)
	return v
}

func (v *AtomicOpCore) Write(factories ...func(ctx context.Context) Op) *AtomicOpCore {
	for _, fn := range factories {
		v.WriteOrErr(func(ctx context.Context) (Op, error) {
			return fn(ctx), nil
		})
	}
	return v
}

func (v *AtomicOpCore) OnWrite(fns ...func(ctx context.Context)) *AtomicOpCore {
	for _, fn := range fns {
		v.WriteOrErr(func(ctx context.Context) (Op, error) {
			fn(ctx)
			return nil, nil
		})
	}
	return v
}

func (v *AtomicOpCore) OnWriteOrErr(fns ...func(ctx context.Context) error) *AtomicOpCore {
	for _, fn := range fns {
		v.WriteOrErr(func(ctx context.Context) (Op, error) {
			return nil, fn(ctx)
		})
	}
	return v
}

func (v *AtomicOpCore) WriteOp(ops ...Op) *AtomicOpCore {
	for _, op := range ops {
		v.Write(func(context.Context) Op {
			return op
		})
	}
	return v
}

func (v *AtomicOpCore) WriteOrErr(factories ...HighLevelFactory) *AtomicOpCore {
	v.writePhase = append(v.writePhase, factories...)
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
		if key := lock.Key(); key == "" || key == "\x00" {
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
		if op, err := opFactory(ctx); err != nil {
			return nil, err
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
		if op, err := opFactory(ctx); err != nil {
			return nil, err
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

// x Create IF part of the transaction.
func (v *AtomicOpCore) writeIfConditions(tracker *TrackerKV, readRev int64) (cmps []etcd.Cmp) {
	for _, op := range tracker.Operations() {
		mustExist := (op.Type == GetOp || op.Type == PutOp) && op.Count > 0
		mustNotExist := op.Type == DeleteOp || op.Count == 0
		switch {
		case mustExist:
			// IF: 0 < modification version <= Read Phase revision
			// Key/range exists and has not been modified since the Read Phase.
			//
			// Note: we cannot check that nothing was deleted from the prefix.
			// The condition IF count == n is needed, and it is not implemented in etcd.
			// We can verify that an individual key was deleted, its MOD == 0.
			cmps = append(cmps,
				// The key/prefix must exist, version must be NOT equal to 0.
				etcd.Cmp{
					Target:      etcdserverpb.Compare_MOD,
					Result:      etcdserverpb.Compare_GREATER,
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
					Key:         op.Key,
					RangeEnd:    op.RangeEnd, // may be empty
				},
				// The key/prefix cannot be modified between GET and UPDATE phase.
				// Mod revision of the item must be less or equal to header.Revision.
				etcd.Cmp{
					Target:      etcdserverpb.Compare_MOD,
					Result:      etcdserverpb.Compare_LESS, // see +1 bellow, so less or equal to header.Revision
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: readRev + 1},
					Key:         op.Key,
					RangeEnd:    op.RangeEnd, // may be empty
				},
			)

			// See SkipPrefixKeysCheck method documentation, by default, the feature is enabled.
			if v.checkPrefixKey {
				if op.RangeEnd != nil {
					for _, kv := range op.KVs {
						cmps = append(cmps, etcd.Cmp{
							Target:      etcdserverpb.Compare_MOD,
							Result:      etcdserverpb.Compare_GREATER,
							TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
							Key:         kv.Key,
						})
					}
				}
			}
		case mustNotExist:
			cmps = append(cmps,
				// IF: modification version == 0
				// The key/range doesn't exist.
				etcd.Cmp{
					Target:      etcdserverpb.Compare_MOD,
					Result:      etcdserverpb.Compare_EQUAL,
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
					Key:         op.Key,
					RangeEnd:    op.RangeEnd, // may be empty
				},
			)
		default:
			panic(errors.Errorf(`unexpected state, operation type "%v"`, op.Type))
		}
	}
	return cmps
}

func (v *AtomicOpCore) do(ctx context.Context, tracker *TrackerKV, opts ...Option) (writeTxn *TxnOp[NoResult], readRevision int64, err error) {
	// Create READ operations
	readTxn, err := v.readTxn(ctx, tracker)
	if err != nil {
		return nil, 0, err
	}

	// Run READ phase, track used keys/prefixes
	readResult := readTxn.Do(ctx, opts...)
	if err = readResult.Err(); err != nil {
		return nil, 0, err
	}

	// Create WRITE transaction
	writeTxn, err = v.writeTxn(ctx)
	if err != nil {
		return nil, 0, err
	}

	return writeTxn, readResult.Header().Revision, nil
}
