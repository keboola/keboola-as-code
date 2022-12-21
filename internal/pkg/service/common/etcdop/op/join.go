package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type JoinTo[R any] struct {
	result *R
	txn    *TxnOp
}

// Join is a wrapper over a transaction mapped to a result R.
// For usage see tests.
func Join[R any](ctx context.Context, result *R, ops ...Op) JoinTo[R] {
	return JoinTo[R]{
		result: result,
		txn:    MergeToTxn(ctx, ops...),
	}
}

func (v JoinTo[R]) Do(ctx context.Context, client *etcd.Client) (R, error) {
	_, err := v.txn.Do(ctx, client)
	if err != nil {
		var empty R
		return empty, err
	}
	return *v.result, nil
}

func (v JoinTo[R]) Op(ctx context.Context) (etcd.Op, error) {
	return v.txn.Op(ctx)
}

func (v JoinTo[R]) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.txn.MapResponse(ctx, response)
}
