package op

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	actualAtomicOpCtxKey  = ctxKey("actualAtomicOp")
	atomicOpMaxReadLevels = 10
)

type ctxKey string

// actualAtomicOp - aux struct, part of the context, to provide actual atomic operation to be extended.
type actualAtomicOp struct {
	Value  *AtomicOpCore
	Closed bool
}

// AtomicFromCtx gets actual atomic operation from the context.
//
// It can be used to add some additional READ operations based on result from a previous READ operation.
// See AtomicOp.Do method and TestAtomicFromCtx_Complex for details.
func AtomicFromCtx(ctx context.Context) *AtomicOpCore {
	actualOp, ok := ctx.Value(actualAtomicOpCtxKey).(*actualAtomicOp)
	if !ok {
		panic(errors.New("no atomic operation found in the context"))
	}
	if actualOp.Closed {
		panic(errors.New("atomic operation in the context is closed"))
	}
	return actualOp.Value
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
	level := 0
	firstReadRevision := int64(0)
	tracker := NewTracker(v.client)

	currentLevel := v.Core()
	writeTxn := TxnWithResult(v.client, v.result)
	for {
		// Stop if there is no new operation
		if currentLevel.Empty() {
			break
		}

		// Prevent infinite loop
		if level >= atomicOpMaxReadLevels {
			return newErrorTxnResult[R](errors.Errorf(`exceeded the maximum number "%d" of read levels in an atomic operation`, atomicOpMaxReadLevels))
		}

		// Create a new empty container: for operations generated during the processing of the current level
		nextLevel := &actualAtomicOp{Value: currentLevel.newEmpty()}

		// Invoke current read level and merge generated partial write txn
		ctx := context.WithValue(ctx, actualAtomicOpCtxKey, nextLevel)
		if partialWriteTxn, revision, err := currentLevel.do(ctx, tracker, opts...); err == nil {
			if firstReadRevision == 0 {
				firstReadRevision = revision
			}
			writeTxn.Merge(partialWriteTxn)
		} else {
			return newErrorTxnResult[R](err)
		}

		// Go to the next level
		level++
		nextLevel.Closed = true // no operation can be added more
		currentLevel = nextLevel.Value
	}

	writeTxn.If(v.writeIfConditions(tracker, firstReadRevision)...)

	// Do whole write txn
	return writeTxn.Do(ctx)
}

// writeIfConditions create IF part of the transaction.
// For every operation from the READ phase,
// IF conditions are generated to check that the state of the key/prefix has not changed.
func (v *AtomicOp[R]) writeIfConditions(tracker *TrackerKV, readRev int64) (cmps []etcd.Cmp) {
	for _, op := range tracker.Operations() {
		mustExist := (op.Type == GetOp || op.Type == PutOp) && op.Count > 0
		mustNotExist := op.Type == DeleteOp || op.Count == 0
		switch {
		case mustExist:
			// IF: 0 < modification version <= READ phase revision
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
