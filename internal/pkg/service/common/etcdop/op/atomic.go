package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
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
//
// The atomic operation can be read from the context during generation and extended, see AtomicFromCtx function.
// This can lead to several subsequent read phases, but there is always only one write phase, after all read phases.
type AtomicOp[R any] struct {
	*AtomicOpCore
	result     *R
	processors processors[R]
}

type AtomicOpInterface interface {
	ReadPhaseOps() []HighLevelFactory
	WritePhaseOps() []HighLevelFactory
	Core() *AtomicOpCore
}

// Mutex abstracts concurrency.Mutex and etcdop.Mutex types.
type Mutex interface {
	Key() string
	IsOwner() etcd.Cmp
}

func Atomic[R any](client etcd.KV, result *R) *AtomicOp[R] {
	v := &AtomicOp[R]{AtomicOpCore: newAtomicCore(client), result: result}
	v.setProcessorFactory(func() func(ctx context.Context, r *TxnResult[NoResult]) {
		if v.processors.len() == 0 {
			return nil
		}

		return func(ctx context.Context, r *TxnResult[NoResult]) {
			if r.Succeeded() || r.Err() != nil {
				v.processors.invoke(ctx, newResult(r.Response(), v.result))
			}
		}
	})
	return v
}

func newAtomicCore(client etcd.KV) *AtomicOpCore {
	return &AtomicOpCore{client: client, checkPrefixKey: true}
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
	v.AtomicOpCore.SkipPrefixKeysCheck()
	return v
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
func (v *AtomicOp[R]) RequireLock(lock Mutex) *AtomicOp[R] {
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

func (v *AtomicOp[R]) OnWrite(fns ...func(ctx context.Context)) *AtomicOp[R] {
	v.AtomicOpCore.OnWrite(fns...)
	return v
}

func (v *AtomicOp[R]) OnWriteOrErr(fns ...func(ctx context.Context) error) *AtomicOp[R] {
	v.AtomicOpCore.OnWriteOrErr(fns...)
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
