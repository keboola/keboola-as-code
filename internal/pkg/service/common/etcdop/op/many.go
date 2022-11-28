package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type GetManyOp = ForType[[]*KeyValue]

// GetManyMapper converts an etcd response to slice of KVs.
type GetManyMapper func(ctx context.Context, r etcd.OpResponse) ([]*KeyValue, error)

// GetManyTMapper converts an etcd response to slice of KVs of the type T.
type GetManyTMapper[T any] func(ctx context.Context, r etcd.OpResponse) (KeyValuesT[T], error)

// NewGetManyOp wraps an operation, the result of which is zero or multiple KV pairs.
func NewGetManyOp(factory Factory, mapper GetManyMapper) GetManyOp {
	return ForType[[]*KeyValue]{factory: factory, mapper: mapper}
}

// NewGetManyTOp wraps an operation, the result of which is zero or multiple KV pairs, values are encoded as the type T.
func NewGetManyTOp[T any](factory Factory, mapper GetManyTMapper[T]) ForType[KeyValuesT[T]] {
	return ForType[KeyValuesT[T]]{factory: factory, mapper: mapper}
}
