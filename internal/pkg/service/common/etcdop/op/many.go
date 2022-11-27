package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// GetManyOp return many results.
type (
	GetManyOp        Op[getManyProcessor]
	getManyProcessor func(ctx context.Context, r etcd.OpResponse) ([]*KeyValue, error)
)

// GetManyTOp returns many typed results.
type (
	GetManyTOp[T any]        Op[getManyTProcessor[T]]
	getManyTProcessor[T any] func(ctx context.Context, r etcd.OpResponse) (KeyValuesT[T], error)
)

// NewGetManyOp wraps an operation, the result of which is zero or multiple KV pairs.
func NewGetManyOp(factory Factory, processor getManyProcessor) GetManyOp {
	return GetManyOp{opFactory: factory, processor: processor}
}

// NewGetManyTOp wraps an operation, the result of which is zero or multiple KV pairs, values are encoded as the type T.
func NewGetManyTOp[T any](factory Factory, processor getManyTProcessor[T]) GetManyTOp[T] {
	return GetManyTOp[T]{opFactory: factory, processor: processor}
}

func (v GetManyOp) Do(ctx context.Context, client *etcd.Client) (kvs []*KeyValue, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}

func (v GetManyTOp[T]) Do(ctx context.Context, client *etcd.Client) (kvs KeyValuesT[T], err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}
