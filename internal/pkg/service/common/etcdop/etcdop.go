// Package etcdop provides a small framework on top of etcd low-level operations.
//
// See Key and Prefix types. Examples can be found in the tests.
//
// Goals:
// - Reduce the risk of an error when defining an operation.
// - Distinguish between operations over one key (Key type) and several keys (Prefix type).
package etcdop

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

// op wraps etcd.Op and adds Op method, so each operation can be unwrapped to raw etcd.Op.
type (
	op                etcd.Op
	boolProcessor     func(r etcd.OpResponse) (bool, error)
	getOneProcessor   func(r etcd.OpResponse) (*mvccpb.KeyValue, error)
	getManyProcessor  func(r etcd.OpResponse) ([]*mvccpb.KeyValue, error)
	noResultProcessor func(r etcd.OpResponse) error
	countProcessor    func(r etcd.OpResponse) int64
	BoolOp            struct {
		op
		processor boolProcessor
	}
	GetOneOp struct {
		op
		processor getOneProcessor
	}
	GetManyOp struct {
		op
		processor getManyProcessor
	}
	CountOp struct {
		op
		processor countProcessor
	}
	NoResultOp struct {
		op
		processor noResultProcessor
	}
)

// Op returns raw etcd.Op.
func (v op) Op() etcd.Op {
	return etcd.Op(v)
}

// NewBoolOp wraps an operation, the result of which us true/false value.
// True means success of the operation.
func NewBoolOp(etcdOp etcd.Op, processor boolProcessor) BoolOp {
	return BoolOp{op: op(etcdOp), processor: processor}
}

// NewGetOneOp wraps an operation, the result of which is one KV pair.
func NewGetOneOp(etcdOp etcd.Op, processor getOneProcessor) GetOneOp {
	return GetOneOp{op: op(etcdOp), processor: processor}
}

// NewGetManyOp wraps an operation, the result of which is zero or multiple KV pairs.
func NewGetManyOp(etcdOp etcd.Op, processor getManyProcessor) GetManyOp {
	return GetManyOp{op: op(etcdOp), processor: processor}
}

// NewCountOp wraps an operation, the result of which is a count.
func NewCountOp(etcdOp etcd.Op, processor countProcessor) CountOp {
	return CountOp{op: op(etcdOp), processor: processor}
}

// NewNoResultOp wraps an operation, the result of which is an error or nil.
func NewNoResultOp(etcdOp etcd.Op, processor noResultProcessor) NoResultOp {
	return NoResultOp{op: op(etcdOp), processor: processor}
}

func (v BoolOp) Do(ctx context.Context, client *etcd.Client) (result bool, err error) {
	if r, err := client.Do(ctx, v.op.Op()); err == nil {
		return v.processor(r)
	} else {
		return false, err
	}
}

func (v GetOneOp) Do(ctx context.Context, client *etcd.Client) (kv *mvccpb.KeyValue, err error) {
	if r, err := client.Do(ctx, v.op.Op()); err == nil {
		return v.processor(r)
	} else {
		return nil, err
	}
}

func (v GetManyOp) Do(ctx context.Context, client *etcd.Client) (kvs []*mvccpb.KeyValue, err error) {
	if r, err := client.Do(ctx, v.op.Op()); err == nil {
		return v.processor(r)
	} else {
		return nil, err
	}
}

func (v CountOp) Do(ctx context.Context, client *etcd.Client) (count int64, err error) {
	if r, err := client.Do(ctx, v.op.Op()); err == nil {
		return v.processor(r), nil
	} else {
		return 0, err
	}
}

func (v NoResultOp) Do(ctx context.Context, client *etcd.Client) (err error) {
	if r, err := client.Do(ctx, v.op.Op()); err == nil {
		return v.processor(r)
	} else {
		return err
	}
}
