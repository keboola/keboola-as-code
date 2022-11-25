// Package etcdop provides a framework on top of etcd low-level operations.
//
// At first, create a custom prefix using NewPrefix/NewTypedPrefix functions.
// Examples can be found in the tests. See also Key, KeyT[T], Prefix and PrefixT[T] types.
//
// Goals:
// - Reduce the risk of an error when defining an operation.
// - Distinguish between operations over one key (Key type) and several keys (Prefix type).
// - Provides Serialization composed of encode, decode and validate operations.
//
// A new operation can be defined as a:
// New<Operation>(<operation factory>, <response processor>) function.
//
// On Operation.Do(ctx, etcdClient) call :
//  - The <operation factory> is executed, result is an etcd operation.
//  - The etcd operation is executed, result is an etcd response.
//  - The <response processor> is executed, to process the etcd response.
//
// If an error occurs, it will be returned and the operation will stop.

package etcdop

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

// op wraps etcd.Op and adds Op method, so each operation can be unwrapped to raw etcd.Op.
type (
	// KeyValue - operation result.
	KeyValue = mvccpb.KeyValue
	// KeyValueT - typed operation result.
	KeyValueT[T any] struct {
		Value T
		KV    *KeyValue
	}
	// Serialization encapsulates serialization and deserialization process of a value.
	Serialization struct {
		// encode a typed value to the etcd KV.
		encode encodeFn
		// decode a typed value to the etcd KV.
		decode decodeFn
		// validate a value before encode and after decode operation.
		validate validateFn
	}
	encodeFn   func(ctx context.Context, value any) (string, error)
	decodeFn   func(ctx context.Context, data []byte, target any) error
	validateFn func(ctx context.Context, value any) error
	// Op is a generic operation.
	Op[P any] struct {
		opFactory opFactory
		processor P
	}
	opFactory func(ctx context.Context) (*etcd.Op, error)
	// BoolOp returns true/false result.
	BoolOp        Op[boolProcessor]
	boolProcessor func(ctx context.Context, r etcd.OpResponse) (bool, error)
	// GetOneOp returns one result.
	GetOneOp        Op[getOneProcessor]
	getOneProcessor func(ctx context.Context, r etcd.OpResponse) (*KeyValue, error)
	// GetOneTOp returns one typed result.
	GetOneTOp[T any]        Op[getOneTProcessor[T]]
	getOneTProcessor[T any] func(ctx context.Context, r etcd.OpResponse) (*KeyValueT[T], error)
	// GetManyOp return many results.
	GetManyOp        Op[getManyProcessor]
	getManyProcessor func(ctx context.Context, r etcd.OpResponse) ([]*KeyValue, error)
	// GetManyTOp returns many typed results.
	GetManyTOp[T any]        Op[getManyTProcessor[T]]
	getManyTProcessor[T any] func(ctx context.Context, r etcd.OpResponse) ([]KeyValueT[T], error)
	// CountOp returns keys count.
	CountOp           Op[countProcessor]
	noResultProcessor func(ctx context.Context, r etcd.OpResponse) error
	// NoResultOp returns only error, if any.
	NoResultOp     Op[noResultProcessor]
	countProcessor func(ctx context.Context, r etcd.OpResponse) int64
)

func NewSerialization(encode encodeFn, decode decodeFn, validate validateFn) Serialization {
	return Serialization{encode: encode, decode: decode, validate: validate}
}

// NewBoolOp wraps an operation, the result of which us true/false value.
// True means success of the operation.
func NewBoolOp(factory opFactory, processor boolProcessor) BoolOp {
	return BoolOp{opFactory: factory, processor: processor}
}

// NewGetOneOp wraps an operation, the result of which is one KV pair.
func NewGetOneOp(factory opFactory, processor getOneProcessor) GetOneOp {
	return GetOneOp{opFactory: factory, processor: processor}
}

// NewGetOneTOp wraps an operation, the result of which is one KV pair, value is encoded as the type T.
func NewGetOneTOp[T any](factory opFactory, processor getOneTProcessor[T]) GetOneTOp[T] {
	return GetOneTOp[T]{opFactory: factory, processor: processor}
}

// NewGetManyOp wraps an operation, the result of which is zero or multiple KV pairs.
func NewGetManyOp(factory opFactory, processor getManyProcessor) GetManyOp {
	return GetManyOp{opFactory: factory, processor: processor}
}

// NewGetManyTOp wraps an operation, the result of which is zero or multiple KV pairs, values are encoded as the type T.
func NewGetManyTOp[T any](factory opFactory, processor getManyTProcessor[T]) GetManyTOp[T] {
	return GetManyTOp[T]{opFactory: factory, processor: processor}
}

// NewCountOp wraps an operation, the result of which is a count.
func NewCountOp(factory opFactory, processor countProcessor) CountOp {
	return CountOp{opFactory: factory, processor: processor}
}

// NewNoResultOp wraps an operation, the result of which is an error or nil.
func NewNoResultOp(factory opFactory, processor noResultProcessor) NoResultOp {
	return NoResultOp{opFactory: factory, processor: processor}
}

// Op returns raw etcd.Op.
func (v opFactory) Op(ctx context.Context) (*etcd.Op, error) {
	return v(ctx)
}

func (v BoolOp) Do(ctx context.Context, client *etcd.Client) (result bool, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return false, err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return false, err
	} else {
		return v.processor(ctx, r)
	}
}

func (v GetOneOp) Do(ctx context.Context, client *etcd.Client) (kv *KeyValue, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}

func (v GetOneTOp[T]) Do(ctx context.Context, client *etcd.Client) (kv *KeyValueT[T], err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}

func (v GetManyOp) Do(ctx context.Context, client *etcd.Client) (kvs []*KeyValue, err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
}

func (v GetManyTOp[T]) Do(ctx context.Context, client *etcd.Client) (kvs []KeyValueT[T], err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return nil, err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return nil, err
	} else {
		return v.processor(ctx, r)
	}
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

func (v NoResultOp) Do(ctx context.Context, client *etcd.Client) (err error) {
	if etcdOp, err := v.opFactory(ctx); err != nil {
		return err
	} else if r, err := client.Do(ctx, *etcdOp); err != nil {
		return err
	} else {
		return v.processor(ctx, r)
	}
}

func (v Serialization) validateAndEncode(ctx context.Context, value any) (string, error) {
	if err := v.validate(ctx, value); err != nil {
		return "", err
	}
	return v.encode(ctx, value)
}

func (v Serialization) decodeAndValidate(ctx context.Context, kv *KeyValue, target any) error {
	if err := v.decode(ctx, kv.Value, target); err != nil {
		return err
	}
	if err := v.validate(ctx, target); err != nil {
		return err
	}
	return nil
}
