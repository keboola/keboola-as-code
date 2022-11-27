package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// NoResultOp returns only error, if any.
type (
	NoResultOp     Op[noResultProcessor]
	countProcessor func(ctx context.Context, r etcd.OpResponse) int64
)

// NewNoResultOp wraps an operation, the result of which is an error or nil.
func NewNoResultOp(factory Factory, processor noResultProcessor) NoResultOp {
	return NoResultOp{opFactory: factory, processor: processor}
}

func (v NoResultOp) Do(ctx context.Context, client *etcd.Client) (err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return err
	} else if r, err := client.Do(ctx, etcdOp); err != nil {
		return err
	} else {
		return v.processor(ctx, r)
	}
}
