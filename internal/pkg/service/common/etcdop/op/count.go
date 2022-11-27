package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// CountOp returns keys count.
type (
	CountOp           Op[countProcessor]
	noResultProcessor func(ctx context.Context, r etcd.OpResponse) error
)

// NewCountOp wraps an operation, the result of which is a count.
func NewCountOp(factory Factory, processor countProcessor) CountOp {
	return CountOp{opFactory: factory, processor: processor}
}

func (v CountOp) Do(ctx context.Context, client *etcd.Client) (count int64, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return 0, err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return 0, err
	} else {
		return v.processor(ctx, r), nil
	}
}
