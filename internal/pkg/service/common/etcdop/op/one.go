package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type GetOneOp = ForType[*KeyValue]

// GetOneMapper converts an etcd response to one KV value or nil.
type GetOneMapper func(ctx context.Context, r etcd.OpResponse) (*KeyValue, error)

// GetOneTProcessor converts an etcd response to one KV of the type T or nil.
type GetOneTProcessor[T any] func(ctx context.Context, r etcd.OpResponse) (*KeyValueT[T], error)

// NewGetOneOp wraps an operation, the result of which is one KV pair.
func NewGetOneOp(factory Factory, mapper GetOneMapper) GetOneOp {
	return ForType[*KeyValue]{factory: factory, mapper: mapper}
}

// NewGetOneTOp wraps an operation, the result of which is one KV pair, value is encoded as the type T.
func NewGetOneTOp[T any](factory Factory, mapper GetOneTProcessor[T]) ForType[*KeyValueT[T]] {
	return ForType[*KeyValueT[T]]{factory: factory, mapper: mapper}
}
