package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type BoolOp = WithResult[bool]

// BoolMapper converts an etcd response to true/false value.
type BoolMapper func(ctx context.Context, r *RawResponse) (bool, error)

// NewBoolOp wraps an operation, the result of which us true/false value.
// True means success of the operation.
func NewBoolOp(client etcd.KV, factory LowLevelFactory, mapper BoolMapper) BoolOp {
	return NewForType[bool](client, factory, mapper)
}
