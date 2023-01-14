package op

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"
)

type JoinTo[R any] struct {
	result *R
	txn    *TxnOpDef
}

// Join is a wrapper over a transaction mapped to a result R.
// For usage see tests.
func Join[R any](result *R, ops ...Op) *JoinTo[R] {
	return &JoinTo[R]{
		result: result,
		txn:    MergeToTxn(ops...),
	}
}

func (v *JoinTo[R]) WithProcessor(p func(context.Context, *R, error) error) *JoinTo[R] {
	v.txn.WithProcessor(func(ctx context.Context, _ *etcd.TxnResponse, _ TxnResult, err error) error {
		return p(ctx, v.result, err)
	})
	return v
}

// WithOnResult is a shortcut for the WithProcessor.
func (v *JoinTo[R]) WithOnResult(fn func(result *R)) *JoinTo[R] {
	return v.WithProcessor(func(_ context.Context, result *R, err error) error {
		if err == nil {
			fn(result)
		}
		return err
	})
}

// WithOnResultOrErr is a shortcut for the WithProcessor.
func (v *JoinTo[R]) WithOnResultOrErr(fn func(result *R) error) *JoinTo[R] {
	return v.WithProcessor(func(_ context.Context, result *R, err error) error {
		if err == nil {
			err = fn(result)
		}
		return err
	})
}

func (v *JoinTo[R]) Op(ctx context.Context) (etcd.Op, error) {
	return v.txn.Op(ctx)
}

func (v *JoinTo[R]) Do(ctx context.Context, client *etcd.Client) (R, error) {
	_, err := v.txn.Do(ctx, client)
	if err != nil {
		var empty R
		return empty, err
	}
	return *v.result, nil
}

func (v *JoinTo[R]) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.txn.MapResponse(ctx, response)
}

func (v *JoinTo[R]) DoWithHeader(ctx context.Context, client etcd.KV, opts ...Option) (*etcdserverpb.ResponseHeader, error) {
	op, err := v.Op(ctx)
	if err != nil {
		return nil, err
	}
	response, err := DoWithRetry(ctx, client, op, opts...)
	return getResponseHeader(response), err
}

func (v *JoinTo[R]) DoOrErr(ctx context.Context, client etcd.KV, opts ...Option) error {
	_, err := v.DoWithHeader(ctx, client, opts...)
	return err
}
