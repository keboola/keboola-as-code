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
	WithResult[NoResult]
}

// NoResultMapper checks and converts raw etcd response to an error or nil.
type NoResultMapper func(ctx context.Context, raw *RawResponse) error

// NewNoResultOp wraps an operation, the result of which is an error or nil.
func NewNoResultOp(client etcd.KV, factory LowLevelFactory, mapper NoResultMapper) NoResultOp {
	out := NoResultOp{}
	out.client = client
	out.factory = factory
	out.mapper = func(ctx context.Context, raw *RawResponse) (NoResult, error) {
		err := mapper(ctx, raw)
		return NoResult{}, err
	}
	return out
}

func (v NoResultOp) Do(ctx context.Context) *Result[NoResult] {
	return v.WithResult.Do(ctx)
}
