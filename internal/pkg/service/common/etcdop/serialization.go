package etcdop

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

// Serialization encapsulates serialization and deserialization process of a value.
type Serialization struct {
	// encode a typed value to the etcd KV.
	encode encodeFn
	// decode a typed value to the etcd KV.
	decode decodeFn
	// validate a value before encode and after decode operation.
	validate validateFn
}

type encodeFn func(ctx context.Context, value any) (string, error)

type decodeFn func(ctx context.Context, data []byte, target any) error

type validateFn func(ctx context.Context, value any) error

func NewSerialization(encode encodeFn, decode decodeFn, validate validateFn) Serialization {
	return Serialization{encode: encode, decode: decode, validate: validate}
}

func (v Serialization) validateAndEncode(ctx context.Context, value any) (string, error) {
	if err := v.validate(ctx, value); err != nil {
		return "", err
	}
	return v.encode(ctx, value)
}

func (v Serialization) decodeAndValidate(ctx context.Context, kv *op.KeyValue, target any) error {
	if err := v.decode(ctx, kv.Value, target); err != nil {
		return err
	}
	if err := v.validate(ctx, target); err != nil {
		return err
	}
	return nil
}
