package op

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AtomicOp is similar to the TxnOp and wraps it.
//
// It can be used to define an atomic operation, that cannot be defined within a single etcd transaction.
// For example some "read for/and update" operation, if we want to read some value, partially modify it and put it back.
//
// The AtomicOp consists of a Read and a Write phase.
//
// Read phase loads keys on which the Write phase depends.
// Between Read and Write phase, the state of the keys in the etcd must not change.
//
// The Read phase collects a list of used keys/prefixes via the TrackerKV utility.
//
// Then the Write phase is started. It is wrapped to a TxnOp with generated IF conditions.
// These conditions check that state obtained by the Read phase has not been modified.
//
// If a modification is detected (txn.Succeed=false):
// - The Do method retries the Read (!) and Write phases.
// - The DoWithoutRetry method returns false.
//
// Retries on network errors are always performed.
type AtomicOp[R any] struct {
	*AtomicOpCore
	result         *R
	processors     processors[R]
	checkPrefixKey bool // checkPrefixKey - see SkipPrefixKeysCheck method documentation
}

// mutex abstracts concurrency.Mutex and etcdop.Mutex types.
type mutex interface {
	Key() string
	IsOwner() etcd.Cmp
}

type AtomicOpInterface interface {
	ReadPhaseOps() []HighLevelFactory
	WritePhaseOps() []HighLevelFactory
}

func Atomic[R any](client etcd.KV, result *R) *AtomicOp[R] {
	return &AtomicOp[R]{AtomicOpCore: &AtomicOpCore{client: client}, result: result, checkPrefixKey: true}
}

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
func (v *AtomicOp[R]) SkipPrefixKeysCheck() *AtomicOp[R] {
	v.checkPrefixKey = false
	return v
}

func (v *AtomicOp[R]) ReadPhaseOps() (out []HighLevelFactory) {
	out = append(out, v.readPhase...)
	return out
}

func (v *AtomicOp[R]) WritePhaseOps() (out []HighLevelFactory) {
	if v.processors.len() == 0 {
		// There is no processor callback, we can pass write phase as is
		out = append(out, v.writePhase...)
	} else {
		out = append(out, func(ctx context.Context) (Op, error) { return v.writeTxn(ctx) })
	}
	return out
}

// Core returns a common interface of the atomic operation, without result type specific methods.
// It is useful when you need to use some helper/hook to modify atomic operations with different result types.
func (v *AtomicOp[R]) Core() *AtomicOpCore {
	return v.AtomicOpCore
}

func (v *AtomicOp[R]) AddFrom(ops ...AtomicOpInterface) *AtomicOp[R] {
	v.AtomicOpCore.AddFrom(ops...)
	return v
}

// RequireLock to run the operation. Internally, an IF condition is generated for each registered lock.
//
// The lock must be locked during the entire operation, otherwise the NotLockedError occurs.
// This signals an error in the application logic.
//
// If the local state of the lock does not match the state in the database (edge case), then the LockedError occurs.
// There are no automatic retries. Depending on the kind of the operation, you may retry or ignore the error.
//
// The method ensures that only the owner of the lock performs the database operation.
func (v *AtomicOp[R]) RequireLock(lock mutex) *AtomicOp[R] {
	v.AtomicOpCore.RequireLock(lock)
	return v
}

func (v *AtomicOp[R]) ReadOp(ops ...Op) *AtomicOp[R] {
	v.AtomicOpCore.ReadOp(ops...)
	return v
}

func (v *AtomicOp[R]) Read(factories ...func(ctx context.Context) Op) *AtomicOp[R] {
	v.AtomicOpCore.Read(factories...)
	return v
}

func (v *AtomicOp[R]) OnRead(fns ...func(ctx context.Context)) *AtomicOp[R] {
	v.AtomicOpCore.OnRead(fns...)
	return v
}

func (v *AtomicOp[R]) OnReadOrErr(fns ...func(ctx context.Context) error) *AtomicOp[R] {
	v.AtomicOpCore.OnReadOrErr(fns...)
	return v
}

func (v *AtomicOp[R]) ReadOrErr(factories ...HighLevelFactory) *AtomicOp[R] {
	v.AtomicOpCore.ReadOrErr(factories...)
	return v
}

func (v *AtomicOp[R]) Write(factories ...func(ctx context.Context) Op) *AtomicOp[R] {
	v.AtomicOpCore.Write(factories...)
	return v
}

func (v *AtomicOp[R]) BeforeWrite(fns ...func(ctx context.Context)) *AtomicOp[R] {
	v.AtomicOpCore.BeforeWrite(fns...)
	return v
}

func (v *AtomicOp[R]) BeforeWriteOrErr(fns ...func(ctx context.Context) error) *AtomicOp[R] {
	v.AtomicOpCore.BeforeWriteOrErr(fns...)
	return v
}

func (v *AtomicOp[R]) WriteOp(ops ...Op) *AtomicOp[R] {
	v.AtomicOpCore.WriteOp(ops...)
	return v
}

func (v *AtomicOp[R]) WriteOrErr(factories ...HighLevelFactory) *AtomicOp[R] {
	v.AtomicOpCore.WriteOrErr(factories...)
	return v
}

// AddProcessor registers a processor callback that can read and modify the result.
// Processor IS NOT executed when the request to database fails.
// Processor IS executed if a logical error occurs, for example, one generated by a previous processor.
// Other Add* methods, shortcuts for AddProcessor, are not executed on logical errors (Result.Err() != nil).
func (v *AtomicOp[R]) AddProcessor(fn func(ctx context.Context, result *Result[R])) *AtomicOp[R] {
	v.processors = v.processors.WithProcessor(fn)
	return v
}

// SetResultTo is a shortcut for the AddProcessor.
// If no error occurred, the result of the operation is written to the target pointer,
// otherwise an empty value is written.
func (v *AtomicOp[R]) SetResultTo(ptr *R) *AtomicOp[R] {
	v.processors = v.processors.WithResultTo(ptr)
	return v
}

// AddResultValidator is a shortcut for the AddProcessor.
// If no error occurred yet, then the callback can validate the result and return an error.
func (v *AtomicOp[R]) AddResultValidator(fn func(R) error) *AtomicOp[R] {
	v.processors = v.processors.WithResultValidator(fn)
	return v
}

// OnResult is a shortcut for the AddProcessor.
// If no error occurred yet, then the callback is executed with the result.
func (v *AtomicOp[R]) OnResult(fn func(result R)) *AtomicOp[R] {
	v.processors = v.processors.WithOnResult(fn)
	return v
}

// EmptyResultAsError is a shortcut for the AddProcessor.
// If no error occurred yet and the result is an empty value for the R type (nil if it is a pointer),
// then the callback is executed and returned error is added to the Result.
func (v *AtomicOp[R]) EmptyResultAsError(fn func() error) *AtomicOp[R] {
	v.processors = v.processors.WithEmptyResultAsError(fn)
	return v
}

func (v *AtomicOp[R]) Do(ctx context.Context, opts ...Option) AtomicResult[R] {
	b := newBackoff(opts...)
	attempt := 0

	var ok bool
	var err error

	for {
		txnResult := v.DoWithoutRetry(ctx, opts...)
		ok = txnResult.Succeeded()
		err = txnResult.Err()
		if err == nil && !ok {
			attempt++
			if delay := b.NextBackOff(); delay == backoff.Stop {
				break
			} else {
				<-time.After(delay)
			}
		} else {
			break
		}
	}

	elapsedTime := b.GetElapsedTime()
	if err == nil && !ok {
		err = errors.Errorf(
			`atomic update failed: revision has been modified between GET and UPDATE op, attempt %d, elapsed time %s`,
			attempt, elapsedTime,
		)
	}

	return AtomicResult[R]{result: v.result, error: err, attempt: attempt, elapsedTime: elapsedTime}
}

func (v *AtomicOp[R]) DoWithoutRetry(ctx context.Context, opts ...Option) *TxnResult[R] {
	tracker := NewTracker(v.client)

	// Create READ operations
	readTxn, err := v.readTxn(ctx, tracker)
	if err != nil {
		return newErrorTxnResult[R](err)
	}

	// Run READ phase, track used keys/prefixes
	readResult := readTxn.Do(ctx, opts...)
	if err := readResult.Err(); err != nil {
		return newErrorTxnResult[R](err)
	}

	// Create WRITE transaction
	writeTxn, err := v.writeTxn(ctx)
	if err != nil {
		return newErrorTxnResult[R](err)
	}

	// Add IF conditions
	writeTxn.If(v.writeIfConditions(tracker, readResult.Header().Revision)...)

	// Do
	return writeTxn.Do(ctx)
}

func (v *AtomicOp[R]) readTxn(ctx context.Context, tracker *TrackerKV) (*TxnOp[NoResult], error) {
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
	lockIfs, err := v.locksIfs()
	if err != nil {
		return nil, errors.PrefixError(err, "read phase")
	}
	readTxn.Merge(Txn(v.client).
		If(lockIfs...).
		OnFailed(func(r *TxnResult[NoResult]) {
			r.AddErr(errors.PrefixError(LockedError{}, "read phase"))
		}),
	)

	return readTxn, nil
}

func (v *AtomicOp[R]) writeTxn(ctx context.Context) (*TxnOp[R], error) {
	// Create WRITE transaction
	writeTxn := TxnWithResult[R](v.client, v.result)
	for _, opFactory := range v.writePhase {
		if op, err := opFactory(ctx); err != nil {
			return nil, err
		} else if op != nil {
			writeTxn.Merge(op)
		}
	}

	// Processors are invoked if the transaction succeeded or there is an error.
	// If the transaction failed, the atomic operation is retried, see Do method.
	writeTxn.AddProcessor(func(ctx context.Context, r *TxnResult[R]) {
		if r.Succeeded() || r.Err() != nil {
			v.processors.invoke(ctx, r.result)
		}
	})

	// Stop the WRITE phase, if a lock is not locked
	lockIfs, err := v.locksIfs()
	if err != nil {
		return nil, errors.PrefixError(err, "write phase")
	}
	writeTxn.Merge(Txn(v.client).
		If(lockIfs...).
		OnFailed(func(r *TxnResult[NoResult]) {
			r.AddErr(errors.PrefixError(LockedError{}, "write phase"))
		}),
	)

	return writeTxn, nil
}

func (v *AtomicOp[R]) locksIfs() (cmps []etcd.Cmp, err error) {
	for _, lock := range v.locks {
		if key := lock.Key(); key == "" || key == "\x00" {
			return nil, NotLockedError{}
		}
		cmps = append(cmps, lock.IsOwner())
	}
	return cmps, nil
}

// x Create IF part of the transaction.
func (v *AtomicOp[R]) writeIfConditions(tracker *TrackerKV, readRev int64) (cmps []etcd.Cmp) {
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
