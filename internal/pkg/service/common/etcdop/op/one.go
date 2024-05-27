package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type GetOneOp = WithResult[*KeyValue]

// GetOneMapper converts an etcd response to one KV value or nil.
type GetOneMapper func(ctx context.Context, raw *RawResponse) (*KeyValue, error)

// GetOneTProcessor converts an etcd response to one KV of the type T or nil.
type GetOneTProcessor[T any] func(ctx context.Context, raw *RawResponse) (*KeyValueT[T], error)

// NewGetOneOp wraps an operation, the result of which is one KV pair.
func NewGetOneOp(client etcd.KV, factory LowLevelFactory, mapper GetOneMapper) GetOneOp {
	return NewForType[*KeyValue](client, factory, mapper)
}

// NewGetOneTOp wraps an operation, the result of which is one KV pair, value is encoded as the type T.
func NewGetOneTOp[T any](client etcd.KV, factory LowLevelFactory, mapper GetOneTProcessor[T]) WithResult[*KeyValueT[T]] {
	return NewForType[*KeyValueT[T]](client, factory, mapper)
}
