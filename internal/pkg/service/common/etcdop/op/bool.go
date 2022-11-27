package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// BoolOp returns true/false result.
type (
	BoolOp        Op[boolProcessor]
	boolProcessor func(ctx context.Context, r etcd.OpResponse) (bool, error)
)

// NewBoolOp wraps an operation, the result of which us true/false value.
// True means success of the operation.
func NewBoolOp(factory Factory, processor boolProcessor) BoolOp {
	return BoolOp{opFactory: factory, processor: processor}
}

func (v BoolOp) Do(ctx context.Context, client *etcd.Client) (result bool, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return false, err
	} else if r, err := client.Do(ctx, etcdOp); err != nil {
		return false, err
	} else {
		return v.processor(ctx, r)
	}
}
