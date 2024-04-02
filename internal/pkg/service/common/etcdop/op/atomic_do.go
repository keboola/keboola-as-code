package op

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

const (
	atomicCoreOpCtxKey    = ctxKey("atomicCoreOp")
	atomicOpMaxReadLevels = 10
)

type ctxKey string

func AtomicFromCtx(ctx context.Context) *AtomicOpCore {
	value, ok := ctx.Value(atomicCoreOpCtxKey).(*AtomicOpCore)
	if !ok {
		panic(errors.New("context is not connected to a running atomic operation"))
	}
	return value
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
		nextLevel := currentLevel.newEmpty()
		ctx := context.WithValue(ctx, atomicCoreOpCtxKey, nextLevel)

		// Invoke current read level and merge generated partial write txn
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
		currentLevel = nextLevel
	}

	writeTxn.If(v.writeIfConditions(tracker, firstReadRevision)...)

	// Do whole write txn
	return writeTxn.Do(ctx)
}
