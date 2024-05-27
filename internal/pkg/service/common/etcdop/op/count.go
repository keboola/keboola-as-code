package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type CountOp = WithResult[int64]

// CountMapper an etcd response to a count.
type CountMapper func(ctx context.Context, raw *RawResponse) (int64, error)

// NewCountOp wraps an operation, the result of which is a count.
func NewCountOp(client etcd.KV, factory LowLevelFactory, mapper CountMapper) CountOp {
	return NewForType[int64](client, factory, mapper)
}
