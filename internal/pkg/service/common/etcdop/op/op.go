// Package op wraps low-level etcd operations to more easily usable high-level operation.
package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// Op is common interface for all operations in the package.
// Each operation also implements "Do" and "WithProcessor" method,
// but they differ in types of the return values.
type Op interface {
	Op(ctx context.Context) (etcd.Op, error)
	MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error)
}

// ForType is generic type for all typed operations in the package.
type ForType[R any] struct {
	factory
	mapper     func(ctx context.Context, r etcd.OpResponse) (R, error)
	processors []func(ctx context.Context, response etcd.OpResponse, result R, err error) (R, error)
}

// Factory creates an etcd operation.
type Factory func(ctx context.Context) (etcd.Op, error)

type factory = Factory

// Op returns raw etcd.Op.
func (v Factory) Op(ctx context.Context) (etcd.Op, error) {
	return v(ctx)
}

func (v ForType[R]) WithProcessor(p func(ctx context.Context, response etcd.OpResponse, result R, err error) (R, error)) ForType[R] {
	v.processors = append(v.processors, p)
	return v
}

func (v ForType[R]) MapResponse(ctx context.Context, response etcd.OpResponse) (any, error) {
	// Same as a part of the "Do" method, but not generic.
	// The method is used in processing of a transaction responses.

	// Map
	result, err := v.mapper(ctx, response)

	// Invoke processors
	for _, p := range v.processors {
		result, err = p(ctx, response, result, err)
	}

	return result, err
}

func (v ForType[R]) Do(ctx context.Context, client etcd.KV) (R, error) {
	var empty R

	// Create etcd operation
	etcdOp, err := v.Op(ctx)
	if err != nil {
		return empty, err
	}

	// Do
	response, err := client.Do(ctx, etcdOp)
	if err != nil {
		return empty, err
	}

	// Map
	result, err := v.mapper(ctx, response)

	// Invoke processors
	for _, p := range v.processors {
		result, err = p(ctx, response, result, err)
	}

	return result, err
}
