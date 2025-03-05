package op

import (
	"context"
	"reflect"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	GetOp TrackedOpType = iota + 1
	PutOp
	DeleteOp
)

// TrackerKV wraps etcd.KV interface and tracks all operations.
// List of the executed operations can be obtained by the Operations method.
// This utility is used by the AtomicOp to collect all keys/prefixes used by the AtomicOp.Read phase.
type TrackerKV struct {
	kv  etcd.KV
	ops []TrackedOp
}

type TrackedOp struct {
	Type TrackedOpType
	// Count number of the affected keys
	Count int64
	// Key used in the operation
	Key []byte
	// RangeEnd, optional, used if the WithPrefix or WithRange option is used
	RangeEnd []byte
	// KVs, optional, contains loaded KVs, only for Get operation
	KVs []*mvccpb.KeyValue
}

type TrackedOpType int

type trackedTxn struct {
	etcd.Txn
	ifOps   []etcd.Cmp
	thenOps []etcd.Op
	elseOps []etcd.Op
	kv      *TrackerKV
}

func NewTracker(kv etcd.KV) *TrackerKV {
	return &TrackerKV{kv: kv}
}

func (v *TrackerKV) Operations() []TrackedOp {
	return v.ops
}

func (v *TrackerKV) Do(ctx context.Context, op etcd.Op) (etcd.OpResponse, error) {
	if op.IsTxn() {
		cmps, thenOps, elseOps := op.Txn()
		resp, err := v.Txn(ctx).If(cmps...).Then(thenOps...).Else(elseOps...).Commit()
		return resp.OpResponse(), err
	}

	resp, err := v.kv.Do(ctx, op)
	if err != nil {
		return etcd.OpResponse{}, err
	}

	v.trackOp(op, resp)
	return resp, err
}

func (v *TrackerKV) Put(ctx context.Context, key, val string, opts ...etcd.OpOption) (*etcd.PutResponse, error) {
	r, err := v.Do(ctx, etcd.OpPut(key, val, opts...))
	return r.Put(), err
}

func (v *TrackerKV) Get(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.GetResponse, error) {
	r, err := v.Do(ctx, etcd.OpGet(key, opts...))
	return r.Get(), err
}

func (v *TrackerKV) Delete(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.DeleteResponse, error) {
	r, err := v.Do(ctx, etcd.OpDelete(key, opts...))
	return r.Del(), err
}

func (v *TrackerKV) Compact(ctx context.Context, rev int64, opts ...etcd.CompactOption) (*etcd.CompactResponse, error) {
	return v.kv.Compact(ctx, rev, opts...)
}

func (v *TrackerKV) Txn(ctx context.Context) etcd.Txn {
	return &trackedTxn{Txn: v.kv.Txn(ctx), kv: v}
}

func (v *trackedTxn) If(ops ...etcd.Cmp) etcd.Txn {
	v.Txn.If(ops...)
	v.ifOps = append(v.ifOps, ops...)
	return v
}

func (v *trackedTxn) Then(ops ...etcd.Op) etcd.Txn {
	v.Txn.Then(ops...)
	v.thenOps = append(v.thenOps, ops...)
	return v
}

func (v *trackedTxn) Else(ops ...etcd.Op) etcd.Txn {
	v.Txn.Else(ops...)
	v.elseOps = append(v.elseOps, ops...)
	return v
}

func (v *trackedTxn) Commit() (*etcd.TxnResponse, error) {
	r, err := v.Txn.Commit()
	if err == nil {
		if r.Succeeded {
			v.kv.trackTxn(r.Responses, v.thenOps)
		} else {
			v.kv.trackTxn(r.Responses, v.elseOps)
		}
	}
	return r, err
}

func (v *TrackerKV) track(t TrackedOpType, key, rangeEnd []byte, count int64, kvs []*mvccpb.KeyValue) {
	record := TrackedOp{Type: t, Key: key, RangeEnd: rangeEnd, Count: count, KVs: kvs}
	for _, r := range v.ops {
		if reflect.DeepEqual(r, record) {
			// duplicate
			return
		}
	}
	v.ops = append(v.ops, record)
}

func (v *TrackerKV) trackOp(op etcd.Op, resp etcd.OpResponse) {
	switch {
	case op.IsGet():
		v.track(GetOp, op.KeyBytes(), op.RangeBytes(), resp.Get().Count, resp.Get().Kvs)
	case op.IsDelete():
		v.track(DeleteOp, op.KeyBytes(), op.RangeBytes(), resp.Del().Deleted, nil)
	case op.IsPut():
		v.track(PutOp, op.KeyBytes(), op.RangeBytes(), 1, nil)
	default:
		panic(errors.Errorf("unexpected op: %v", op))
	}
}

// trackTxn nested operations.
// Inspired by etcd-io/etcd/leasing/util.go:gatherResponseOps
// https://github.com/etcd-io/etcd/blob/d37f9b2092a07790195e88c0aedd996fe9c07312/client/v3/leasing/util.go#L83-L97
func (v *TrackerKV) trackTxn(resp []*etcdserverpb.ResponseOp, ops []etcd.Op) {
	for i, op := range ops {
		if !op.IsTxn() {
			switch {
			case op.IsGet():
				v.track(GetOp, op.KeyBytes(), op.RangeBytes(), resp[i].GetResponseRange().Count, resp[i].GetResponseRange().Kvs)
			case op.IsDelete():
				v.track(DeleteOp, op.KeyBytes(), op.RangeBytes(), resp[i].GetResponseDeleteRange().Deleted, nil)
			case op.IsPut():
				v.track(PutOp, op.KeyBytes(), op.RangeBytes(), 1, nil)
			default:
				panic(errors.Errorf("unexpected op: %v", op))
			}
			continue
		}
		_, thenOps, elseOps := op.Txn()
		if txnResp := resp[i].GetResponseTxn(); txnResp.Succeeded {
			v.trackTxn(txnResp.Responses, thenOps)
		} else {
			v.trackTxn(txnResp.Responses, elseOps)
		}
	}
}
