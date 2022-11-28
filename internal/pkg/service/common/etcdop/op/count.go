package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type CountOp = ForType[int64]

// CountMapper an etcd response to a count.
type CountMapper func(ctx context.Context, r etcd.OpResponse) (int64, error)

// NewCountOp wraps an operation, the result of which is a count.
func NewCountOp(factory Factory, mapper CountMapper) CountOp {
	return ForType[int64]{factory: factory, mapper: mapper}
}
