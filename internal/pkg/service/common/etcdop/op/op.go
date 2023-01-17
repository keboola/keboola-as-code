// Package op wraps low-level etcd operations to more easily usable high-level operation.
package op

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Op is common interface for all operations in the package.
// Each operation also implements "Do" and "WithProcessor" method,
// but they differ in types of the return values.
type Op interface {
	Op(ctx context.Context) (etcd.Op, error)
	MapResponse(ctx context.Context, response Response) (result any, err error)
	DoWithHeader(ctx context.Context, client etcd.KV, opts ...Option) (*etcdserverpb.ResponseHeader, error)
	DoOrErr(ctx context.Context, client etcd.KV, opts ...Option) error
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

// WithOnResult is a shortcut for the WithProcessor.
func (v ForType[R]) WithOnResult(fn func(result R)) ForType[R] {
	return v.WithProcessor(func(_ context.Context, _ etcd.OpResponse, result R, err error) (R, error) {
		if err == nil {
			fn(result)
		}
		return result, err
	})
}

// WithOnResultOrErr is a shortcut for the WithProcessor.
func (v ForType[R]) WithOnResultOrErr(fn func(result R) error) ForType[R] {
	return v.WithProcessor(func(_ context.Context, _ etcd.OpResponse, result R, err error) (R, error) {
		if err == nil {
			err = fn(result)
		}
		return result, err
	})
}

func (v ForType[R]) MapResponse(ctx context.Context, response Response) (any, error) {
	// Same as a part of the "Do" method, but not generic.
	// The method is used in processing of a transaction responses.

	// Map
	result, err := v.mapper(ctx, response.OpResponse)

	// Invoke processors
	for _, p := range v.processors {
		result, err = p(ctx, response.OpResponse, result, err)
	}

	return result, err
}

func (v ForType[R]) DoWithRaw(ctx context.Context, client etcd.KV, opts ...Option) (R, etcd.OpResponse, error) {
	var empty R

	// Create etcd operation
	etcdOp, err := v.Op(ctx)
	if err != nil {
		return empty, etcd.OpResponse{}, err
	}

	// Do with retry
	response, err := DoWithRetry(ctx, client, etcdOp, opts...)
	if err != nil {
		return empty, etcd.OpResponse{}, err
	}

	// Map
	result, err := v.mapper(ctx, response)

	// Invoke processors
	for _, p := range v.processors {
		result, err = p(ctx, response, result, err)
	}

	return result, response, err
}

func (v ForType[R]) Do(ctx context.Context, client etcd.KV, opts ...Option) (R, error) {
	r, _, err := v.DoWithRaw(ctx, client, opts...)
	return r, err
}

func (v ForType[R]) DoWithHeader(ctx context.Context, client etcd.KV, opts ...Option) (*etcdserverpb.ResponseHeader, error) {
	_, h, err := v.DoWithRaw(ctx, client, opts...)
	return getResponseHeader(h), err
}

func (v ForType[R]) DoOrErr(ctx context.Context, client etcd.KV, opts ...Option) error {
	_, _, err := v.DoWithRaw(ctx, client, opts...)
	return err
}

func getResponseHeader(response etcd.OpResponse) *etcdserverpb.ResponseHeader {
	var header *etcdserverpb.ResponseHeader
	if v := response.Get(); v != nil {
		header = v.Header
	} else if v := response.Del(); v != nil {
		header = v.Header
	} else if v := response.Put(); v != nil {
		header = v.Header
	} else if v := response.Txn(); v != nil {
		header = v.Header
	} else {
		panic(errors.Errorf(`unexpected response type`))
	}
	return header
}
