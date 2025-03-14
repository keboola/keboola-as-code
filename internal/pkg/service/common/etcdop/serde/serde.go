// Package serde provides encode, decode and validate operations for any value.
package serde

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Serde encapsulates serialization and deserialization process of a value.
type Serde struct {
	// encode a typed value to the etcd KV.
	encode EncodeFn
	// decode a typed value to the etcd KV.
	decode DecodeFn
	// validate a value before encode and after decode operation.
	validate ValidateFn
}

type EncodeFn func(ctx context.Context, value any) (string, error)

type DecodeFn func(ctx context.Context, data []byte, target any) error

type ValidateFn func(ctx context.Context, value any) error

func NoValidation(ctx context.Context, v any) error {
	return nil
}

func New(encode EncodeFn, decode DecodeFn, validate ValidateFn) *Serde {
	return &Serde{encode: encode, decode: decode, validate: validate}
}

func NewJSON(validate ValidateFn) *Serde {
	if validate == nil {
		panic(errors.New("validate fn cannot be nil, use serde.NoValidation"))
	}
	return New(
		func(ctx context.Context, value any) (string, error) {
			return json.EncodeString(value, false)
		},
		func(ctx context.Context, data []byte, target any) error {
			return json.DecodePreserveNumber(data, target)
		},
		validate,
	)
}

func (v Serde) Encode(ctx context.Context, value any) (string, error) {
	if err := v.validate(ctx, value); err != nil {
		return "", err
	}
	return v.encode(ctx, value)
}

func (v Serde) Decode(ctx context.Context, kv *op.KeyValue, target any) error {
	if err := v.decode(ctx, kv.Value, target); err != nil {
		return err
	}
	if err := v.validate(ctx, target); err != nil {
		return err
	}
	return nil
}
