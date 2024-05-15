// Package op wraps low-level etcd operations to more easily usable high-level operation.
//
// Main benefits:
//
//   - Processors/callbacks can be attached to operations.
//   - Operations can be easily merged into transactions, see TxnOp.
//   - AtomicOp provides atomic ReadAndUpdate pattern.
package op

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"
)

// Op is common interface for all operations in the package.
// Each operation also implements "Do" and "WithProcessor" methods,
// but they differ in types of the return values.
type Op interface {
	Op(ctx context.Context) (LowLevelOp, error)
}

type LowLevelOp struct {
	// Op is a low-level etcd operation.
	Op etcd.Op
	// MapResponse is a callback used to map response from the Op.
	MapResponse MapFn
}

type MapFn func(ctx context.Context, raw RawResponse) (result any, err error)

type Header = etcdserverpb.ResponseHeader // shortcut

// WithResult is generic type for all typed operations in the package, it implements the Op interface.
// The R is the operation result type.
// The struct is immutable, see With* methods.
type WithResult[R any] struct {
	client     etcd.KV
	factory    LowLevelFactory
	mapper     func(ctx context.Context, raw RawResponse) (result R, err error)
	processors processors[R]
}

// LowLevelFactory creates a low-level etcd operation.
type LowLevelFactory func(ctx context.Context) (etcd.Op, error)

// HighLevelFactory creates a high-level etcd operation.
//
// The factory can return <nil>, if you want to execute some code during the READ phase,
// but no etcd operation is generated.
//
// The factory can return op.ErrorOp(err) OR op.ErrorTxn[T](err) to signal a static error.
type HighLevelFactory func(ctx context.Context) Op

func NewForType[R any](client etcd.KV, factory LowLevelFactory, mapper func(ctx context.Context, raw RawResponse) (result R, err error)) WithResult[R] {
	return WithResult[R]{client: client, factory: factory, mapper: mapper}
}

func (v WithResult[R]) Op(ctx context.Context) (out LowLevelOp, err error) {
	// Create low-level operation
	if out.Op, err = v.factory(ctx); err != nil {
		return out, err
	}

	// Register response mapper
	out.MapResponse = func(ctx context.Context, raw RawResponse) (result any, err error) {
		return v.mapResponse(ctx, raw).ResultOrErr()
	}

	return out, nil
}

func (v WithResult[R]) Do(ctx context.Context, opts ...Option) *Result[R] {
	// Create low-level operation
	op, err := v.Op(ctx)
	if err != nil {
		return newErrorResult[R](err)
	}

	// Do with retry
	raw, err := DoWithRetry(ctx, v.client, op.Op, opts...)
	if err != nil {
		return newErrorResult[R](err)
	}

	// Map the raw response
	return v.mapResponse(ctx, raw)
}

// WithProcessor registers a processor callback that can read and modify the result.
// Processor IS NOT executed when the request to database fails.
// Processor IS executed if a logical error occurs, for example, one generated by a previous processor.
// Other With* methods, shortcuts for WithProcessor, are not executed on logical errors (Result.Err() != nil).
func (v WithResult[R]) WithProcessor(fn func(ctx context.Context, result *Result[R])) WithResult[R] {
	v.processors = v.processors.WithProcessor(fn)
	return v
}

// WithResultTo is a shortcut for the WithProcessor.
// If no error occurred, the result of the operation is written to the target pointer,
// otherwise an empty value is written.
func (v WithResult[R]) WithResultTo(ptr *R) WithResult[R] {
	v.processors = v.processors.WithResultTo(ptr)
	return v
}

// WithResultValidator is a shortcut for the WithProcessor.
// If no error occurred yet, then the callback can validate the result and return an error.
func (v WithResult[R]) WithResultValidator(fn func(R) error) WithResult[R] {
	v.processors = v.processors.WithResultValidator(fn)
	return v
}

// WithOnResult is a shortcut for the WithProcessor.
// If no error occurred yet, then the callback is executed with the result.
func (v WithResult[R]) WithOnResult(fn func(result R)) WithResult[R] {
	v.processors = v.processors.WithOnResult(fn)
	return v
}

// WithEmptyResultAsError is a shortcut for the WithProcessor.
// If no error occurred yet and the result is an empty value for the R type (nil if it is a pointer),
// then the callback is executed and returned error is added to the Result.
func (v WithResult[R]) WithEmptyResultAsError(fn func() error) WithResult[R] {
	v.processors = v.processors.WithEmptyResultAsError(fn)
	return v
}

func (v WithResult[R]) mapResponse(ctx context.Context, raw RawResponse) *Result[R] {
	// Map response to the result value
	var r *Result[R]
	if value, err := v.mapper(ctx, raw); err == nil {
		r = newResult[R](&raw, &value)
	} else {
		r = newResult[R](&raw, nil).AddErr(err)
	}

	// Invoke WithResult
	v.processors.invoke(ctx, r)

	return r
}
