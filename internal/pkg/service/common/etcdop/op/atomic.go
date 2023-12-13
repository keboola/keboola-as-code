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
	client     etcd.KV
	result     *R
	readPhase  []HighLevelFactory
	writePhase []HighLevelFactory
}

type AtomicResult[R any] struct {
	result      *R
	error       error
	attempt     int
	elapsedTime time.Duration
}

type atomicOpInterface interface {
	ReadPhaseOps() []HighLevelFactory
	WritePhaseOps() []HighLevelFactory
}

func Atomic[R any](client etcd.KV, result *R) *AtomicOp[R] {
	return &AtomicOp[R]{client: client, result: result}
}

func (v *AtomicOp[R]) AddFrom(ops ...atomicOpInterface) *AtomicOp[R] {
	for _, op := range ops {
		v.readPhase = append(v.readPhase, op.ReadPhaseOps()...)
		v.writePhase = append(v.writePhase, op.WritePhaseOps()...)
	}
	return v
}

func (v *AtomicOp[R]) ReadPhaseOps() (out []HighLevelFactory) {
	out = make([]HighLevelFactory, len(v.readPhase))
	copy(out, v.readPhase)
	return out
}

func (v *AtomicOp[R]) WritePhaseOps() (out []HighLevelFactory) {
	out = make([]HighLevelFactory, len(v.writePhase))
	copy(out, v.writePhase)
	return out
}

func (v *AtomicOp[R]) ReadOp(ops ...Op) *AtomicOp[R] {
	for _, op := range ops {
		v.Read(func() Op {
			return op
		})
	}
	return v
}

func (v *AtomicOp[R]) Read(factories ...func() Op) *AtomicOp[R] {
	for _, fn := range factories {
		v.ReadOrErr(func() (Op, error) {
			return fn(), nil
		})
	}
	return v
}

func (v *AtomicOp[R]) OnRead(fns ...func()) *AtomicOp[R] {
	for _, fn := range fns {
		v.ReadOrErr(func() (Op, error) {
			fn()
			return nil, nil
		})
	}
	return v
}

func (v *AtomicOp[R]) OnReadOrErr(fns ...func() error) *AtomicOp[R] {
	for _, fn := range fns {
		v.ReadOrErr(func() (Op, error) {
			return nil, fn()
		})
	}
	return v
}

func (v *AtomicOp[R]) ReadOrErr(factories ...HighLevelFactory) *AtomicOp[R] {
	v.readPhase = append(v.readPhase, factories...)
	return v
}

func (v *AtomicOp[R]) Write(factories ...func() Op) *AtomicOp[R] {
	for _, fn := range factories {
		v.WriteOrErr(func() (Op, error) {
			return fn(), nil
		})
	}
	return v
}

func (v *AtomicOp[R]) BeforeWrite(fns ...func()) *AtomicOp[R] {
	for _, fn := range fns {
		v.WriteOrErr(func() (Op, error) {
			fn()
			return nil, nil
		})
	}
	return v
}

func (v *AtomicOp[R]) BeforeWriteOrErr(fns ...func() error) *AtomicOp[R] {
	for _, fn := range fns {
		v.WriteOrErr(func() (Op, error) {
			return nil, fn()
		})
	}
	return v
}

func (v *AtomicOp[R]) WriteOp(ops ...Op) *AtomicOp[R] {
	for _, op := range ops {
		v.Write(func() Op {
			return op
		})
	}
	return v
}

func (v *AtomicOp[R]) WriteOrErr(factories ...HighLevelFactory) *AtomicOp[R] {
	v.writePhase = append(v.writePhase, factories...)
	return v
}

func (v *AtomicOp[R]) Do(ctx context.Context, opts ...Option) AtomicResult[R] {
	b := newBackoff(opts...)
	attempt := 0

	var ok bool
	var err error

	for {
		if ok, err = v.DoWithoutRetry(ctx, opts...); err != nil {
			break
		} else if !ok {
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

func (v *AtomicOp[R]) DoWithoutRetry(ctx context.Context, opts ...Option) (bool, error) {
	// Create GET operations
	var getOps []Op
	for _, opFactory := range v.readPhase {
		op, err := opFactory()
		if err != nil {
			return false, err
		}
		if op != nil {
			getOps = append(getOps, op)
		}
	}

	// Run GET operation, track used keys/prefixes
	tracker := NewTracker(v.client)
	header, err := NewTxnOp(tracker).Then(getOps...).Do(ctx, opts...).HeaderOrErr()
	if err != nil {
		return false, err
	}

	// Create UPDATE operation
	var updateOps []Op
	for _, opFactory := range v.writePhase {
		op, err := opFactory()
		if err != nil {
			return false, err
		}
		if op != nil {
			updateOps = append(updateOps, op)
		}
	}

	// Create IF part of the transaction
	var cmps []etcd.Cmp
	for _, op := range removeOpsOverlaps(tracker.Operations()) {
		switch op.Type {
		case DeleteOp:
			cmps = append(cmps,
				// The key/prefix must be missing, version must be equal to 0.
				etcd.Cmp{
					Result:      etcdserverpb.Compare_EQUAL,
					Target:      etcdserverpb.Compare_VERSION,
					TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
					Key:         op.Key,
					RangeEnd:    op.RangeEnd, // may be empty
				},
			)
		case GetOp:
			if op.Count > 0 {
				cmps = append(cmps,
					// The key/prefix must exist, version must be NOT equal to 0.
					etcd.Cmp{
						Result:      etcdserverpb.Compare_GREATER,
						Target:      etcdserverpb.Compare_VERSION,
						TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
						Key:         op.Key,
						RangeEnd:    op.RangeEnd, // may be empty
					})
			}
			fallthrough
		case PutOp:
			cmps = append(cmps,
				// The key/prefix cannot be modified between GET and UPDATE phase.
				// Mod revision of the item must be less or equal to header.Revision.
				etcd.Cmp{
					Result:      etcdserverpb.Compare_LESS, // see +1 bellow, so less or equal to header.Revision
					Target:      etcdserverpb.Compare_MOD,
					TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: header.Revision + 1},
					Key:         op.Key,
					RangeEnd:    op.RangeEnd, // may be empty
				})
		default:
			panic(errors.Errorf(`unexpected operation type "%v"`, op.Type))
		}
	}

	// Create transaction
	// IF no key/prefix has been changed, THEN do updateOp
	txnResp := NewTxnOp(v.client).If(cmps...).Then(updateOps...).Do(ctx)
	return txnResp.Succeeded(), txnResp.Err()
}

func (v AtomicResult[R]) Result() R {
	var empty R
	if v.error != nil || v.result == nil {
		return empty
	}
	return *v.result
}

func (v AtomicResult[R]) Err() error {
	return v.error
}

func (v AtomicResult[R]) ResultOrErr() (R, error) {
	var empty R
	if v.error != nil {
		return empty, v.error
	}
	if v.result == nil {
		return empty, nil
	}
	return *v.result, nil
}

func (v AtomicResult[R]) Attempt() int {
	return v.attempt
}

func (v AtomicResult[R]) ElapsedTime() time.Duration {
	return v.elapsedTime
}
