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
type AtomicOp struct {
	readPhase  []func() (Op, error)
	writePhase []func() (Op, error)
}

func Atomic() *AtomicOp {
	return &AtomicOp{}
}

func (v *AtomicOp) AddFrom(ops ...*AtomicOp) *AtomicOp {
	for _, op := range ops {
		v.readPhase = append(v.readPhase, op.readPhase...)
		v.writePhase = append(v.writePhase, op.writePhase...)
	}
	return v
}

func (v *AtomicOp) Read(factories ...func() Op) *AtomicOp {
	for _, fn := range factories {
		v.ReadOrErr(func() (Op, error) {
			return fn(), nil
		})
	}
	return v
}

func (v *AtomicOp) ReadOrErr(factories ...func() (Op, error)) *AtomicOp {
	v.readPhase = append(v.readPhase, factories...)
	return v
}

func (v *AtomicOp) Write(factories ...func() Op) *AtomicOp {
	for _, fn := range factories {
		v.WriteOrErr(func() (Op, error) {
			return fn(), nil
		})
	}
	return v
}

func (v *AtomicOp) WriteOrErr(factories ...func() (Op, error)) *AtomicOp {
	v.writePhase = append(v.writePhase, factories...)
	return v
}

func (v *AtomicOp) Do(ctx context.Context, client etcd.KV, opts ...Option) error {
	b := newBackoff(opts...)
	attempt := 0

	var ok bool
	var err error

	for {
		if ok, err = v.DoWithoutRetry(ctx, client, opts...); err != nil {
			return err
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

	if !ok {
		return errors.Errorf(
			`atomic update failed: revision has been modified between GET and UPDATE op, attempt %d, elapsed time %s`,
			attempt, b.GetElapsedTime(),
		)
	}

	return nil
}

func (v *AtomicOp) DoWithoutRetry(ctx context.Context, client etcd.KV, opts ...Option) (bool, error) {
	// Create GET operations
	var getOps []Op
	for _, opFactory := range v.readPhase {
		op, err := opFactory()
		if err != nil {
			return false, err
		}
		getOps = append(getOps, op)
	}

	// Run GET operation, track used keys/prefixes
	tracker := NewTracker(client)
	header, err := NewTxnOp().Then(getOps...).DoWithHeader(ctx, tracker, opts...)
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
		updateOps = append(updateOps, op)
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
	txnResp, err := NewTxnOp().If(cmps...).Then(updateOps...).Do(ctx, client)
	if err != nil {
		return false, err
	}
	return txnResp.Succeeded, nil
}
