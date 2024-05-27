package etcdop

import (
	"context"
	"strings"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type prefix = Prefix

// Prefix represents an etcd keys prefix - multiple keys prefix, not a one key.
type Prefix string

// PrefixT extends Prefix with generic functionality, contains type of the serialized value.
type PrefixT[T any] struct {
	prefix
	serde *serde.Serde
}

func (v Prefix) Prefix() string {
	if v == "" {
		// Special case - all keys
		return ""
	}
	return string(v) + "/"
}

func (v Prefix) Add(str string) Prefix {
	return Prefix(v.Prefix() + str)
}

func (v Prefix) Key(key string) Key {
	return Key(v.Prefix() + key)
}

func (v PrefixT[T]) Serde() *serde.Serde {
	return v.serde
}

func (v PrefixT[T]) Prefix() string {
	return v.prefix.Prefix()
}

func (v PrefixT[T]) PrefixT() PrefixT[T] {
	return v
}

func (v PrefixT[T]) Add(str string) PrefixT[T] {
	return PrefixT[T]{prefix: v.prefix.Add(str), serde: v.serde}
}

func (v PrefixT[T]) Key(key string) KeyT[T] {
	return KeyT[T]{
		key:   v.prefix.Key(key),
		serde: v.serde,
	}
}

func NewPrefix(v string) Prefix {
	return Prefix(strings.Trim(v, "/"))
}

func NewTypedPrefix[T any](v Prefix, s *serde.Serde) PrefixT[T] {
	return PrefixT[T]{prefix: v, serde: s}
}

func (v Prefix) AtLeastOneExists(client etcd.KV, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCountOnly()}, opts...)
			return etcd.OpGet(v.Prefix(), opts...), nil
		},
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
			return raw.Get().Count > 0, nil
		},
	)
}

func (v Prefix) Count(client etcd.KV, opts ...etcd.OpOption) op.CountOp {
	return op.NewCountOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithCountOnly(), etcd.WithPrefix()}, opts...)
			return etcd.OpGet(v.Prefix(), opts...), nil
		},
		func(_ context.Context, raw *op.RawResponse) (int64, error) {
			return raw.Get().Count, nil
		},
	)
}

func (v Prefix) GetOne(client etcd.KV, opts ...etcd.OpOption) op.GetOneOp {
	return op.NewGetOneOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithLimit(1)}, opts...)
			return etcd.OpGet(v.Prefix(), opts...), nil
		},
		func(ctx context.Context, raw *op.RawResponse) (*op.KeyValue, error) {
			// Not r.Get.Count(), it returns the count of all records, regardless of the limit
			count := len(raw.Get().Kvs)
			switch count {
			case 0:
				return nil, nil
			case 1:
				return raw.Get().Kvs[0], nil
			default:
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v Prefix) GetAll(client etcd.KV, opts ...iterator.Option) iterator.Definition {
	return iterator.New(client, v.Prefix(), opts...)
}

func (v Prefix) DeleteAll(client etcd.KV, opts ...etcd.OpOption) op.CountOp {
	return op.NewCountOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
			return etcd.OpDelete(v.Prefix(), opts...), nil
		},
		func(_ context.Context, raw *op.RawResponse) (int64, error) {
			return raw.Del().Deleted, nil
		},
	)
}

func (v PrefixT[T]) GetOne(client etcd.KV, opts ...etcd.OpOption) op.WithResult[*op.KeyValueT[T]] {
	return op.NewGetOneTOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithLimit(1)}, opts...)
			return etcd.OpGet(v.Prefix(), opts...), nil
		},
		func(ctx context.Context, raw *op.RawResponse) (*op.KeyValueT[T], error) {
			// Not r.Get.Count(), it returns the count of all records, regardless of the limit
			count := len(raw.Get().Kvs)
			switch count {
			case 0:
				return nil, nil
			case 1:
				kv := raw.Get().Kvs[0]
				target := new(T)
				if err := v.serde.Decode(ctx, kv, target); err != nil {
					return nil, errors.Errorf("etcd operation \"get one\" failed: %w", invalidValueError(string(kv.Key), err))
				}
				return &op.KeyValueT[T]{Value: *target, Kv: kv}, nil
			default:
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v PrefixT[T]) GetAll(client etcd.KV, opts ...iterator.Option) iterator.DefinitionT[T] {
	return iterator.NewTyped[T](client, v.serde, v.Prefix(), opts...)
}
