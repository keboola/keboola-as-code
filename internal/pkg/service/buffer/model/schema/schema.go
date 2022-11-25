// Package schema defines etcd keys for the Buffer service.
package schema

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type Schema struct {
	serialization etcdop.Serialization
}

type validateFn func(ctx context.Context, value any) error

type prefix = etcdop.Prefix

func New(validate validateFn) *Schema {
	return &Schema{
		serialization: etcdop.NewSerialization(
			func(ctx context.Context, value any) (string, error) {
				return json.EncodeString(value, false)
			},
			func(ctx context.Context, data []byte, target any) error {
				return json.Decode(data, target)
			},
			func(ctx context.Context, value any) error {
				return validate(ctx, value)
			},
		),
	}
}
