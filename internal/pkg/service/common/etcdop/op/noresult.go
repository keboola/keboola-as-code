package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

type NoResult struct{}

// NoResultOp returns only error, if any.
// It is implemented a little differently than other operations,
// because it has only one return value of type error (not two).
type NoResultOp struct {
	ForType[NoResult]
}

// NoResultMapper checks and converts raw etcd response to an error or nil.
type NoResultMapper func(ctx context.Context, r etcd.OpResponse) error

// NewNoResultOp wraps an operation, the result of which is an error or nil.
func NewNoResultOp(factory Factory, mapper NoResultMapper) NoResultOp {
	out := NoResultOp{}
	out.factory = factory
	out.mapper = func(ctx context.Context, r etcd.OpResponse) (NoResult, error) {
		err := mapper(ctx, r)
		return NoResult{}, err
	}
	return out
}

func (v NoResultOp) Do(ctx context.Context, client etcd.KV) (err error) {
	_, err = v.ForType.Do(ctx, client)
	return err
}
