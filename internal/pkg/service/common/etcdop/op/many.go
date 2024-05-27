package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type GetManyOp = WithResult[[]*KeyValue]

// GetManyMapper converts an etcd response to slice of KVs.
type GetManyMapper func(ctx context.Context, raw *RawResponse) ([]*KeyValue, error)

// GetManyTMapper converts an etcd response to slice of KVs of the type T.
type GetManyTMapper[T any] func(ctx context.Context, raw *RawResponse) (KeyValuesT[T], error)

// NewGetManyOp wraps an operation, the result of which is zero or multiple KV pairs.
func NewGetManyOp(client etcd.KV, factory LowLevelFactory, mapper GetManyMapper) GetManyOp {
	return NewForType[[]*KeyValue](client, factory, mapper)
}

// NewGetManyTOp wraps an operation, the result of which is zero or multiple KV pairs, values are encoded as the type T.
func NewGetManyTOp[T any](client etcd.KV, factory LowLevelFactory, mapper GetManyTMapper[T]) WithResult[KeyValuesT[T]] {
	return NewForType[KeyValuesT[T]](client, factory, mapper)
}
