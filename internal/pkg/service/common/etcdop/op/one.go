package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// GetOneOp returns one result.
type (
	GetOneOp        Op[getOneProcessor]
	getOneProcessor func(ctx context.Context, r etcd.OpResponse) (*KeyValue, error)
)

// GetOneTOp returns one typed result.
type (
	GetOneTOp[T any]        Op[getOneTProcessor[T]]
	getOneTProcessor[T any] func(ctx context.Context, r etcd.OpResponse) (*KeyValueT[T], error)
)

// NewGetOneOp wraps an operation, the result of which is one KV pair.
func NewGetOneOp(factory Factory, processor getOneProcessor) GetOneOp {
	return GetOneOp{opFactory: factory, processor: processor}
}

// NewGetOneTOp wraps an operation, the result of which is one KV pair, value is encoded as the type T.
func NewGetOneTOp[T any](factory Factory, processor getOneTProcessor[T]) GetOneTOp[T] {
	return GetOneTOp[T]{opFactory: factory, processor: processor}
}

func (v GetOneOp) Do(ctx context.Context, client *etcd.Client) (kv *KeyValue, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}

func (v GetOneTOp[T]) Do(ctx context.Context, client *etcd.Client) (kv *KeyValueT[T], err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}
