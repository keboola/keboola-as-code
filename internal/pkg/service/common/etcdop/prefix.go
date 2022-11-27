package etcdop

import (
	"context"
	"strings"

	etcd "go.etcd.io/etcd/client/v3"
)

type prefix = Prefix

// Prefix represents an etcd keys prefix - multiple keys prefix, not a one key.
type Prefix string

// PrefixT extends Prefix with generic functionality, contains type of the serialized value.
type PrefixT[T any] struct {
	prefix
	serialization Serialization
}

func (v Prefix) Prefix() string {
	return string(v) + "/"
}

func (v Prefix) Add(str string) Prefix {
	return Prefix(v.Prefix() + str)
}

func (v Prefix) Key(key string) Key {
	return Key(v.Prefix() + key)
}

func (v PrefixT[T]) Prefix() string {
	return v.prefix.Prefix()
}

func (v PrefixT[T]) Add(str string) PrefixT[T] {
	return PrefixT[T]{prefix: v.prefix.Add(str), serialization: v.serialization}
}

func (v PrefixT[T]) Key(key string) KeyT[T] {
	return KeyT[T]{
		key:           v.prefix.Key(key),
		serialization: v.serialization,
	}
}

func NewPrefix(v string) Prefix {
	return Prefix(strings.Trim(v, "/"))
}

func NewTypedPrefix[T any](v Prefix, s Serialization) PrefixT[T] {
	return PrefixT[T]{prefix: v, serialization: s}
}

func (v Prefix) AtLeastOneExists(opts ...etcd.OpOption) BoolOp {
	return NewBoolOp(
		func(_ context.Context) (*etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCountOnly()}, opts...)
			etcdOp := etcd.OpGet(v.Prefix(), opts...)
			return &etcdOp, nil
		},
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			return r.Get().Count > 0, nil
		},
	)
}

func (v Prefix) Count(opts ...etcd.OpOption) CountOp {
	return NewCountOp(
		func(_ context.Context) (*etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithCountOnly(), etcd.WithPrefix()}, opts...)
			etcdOp := etcd.OpGet(v.Prefix(), opts...)
			return &etcdOp, nil
		},
		func(_ context.Context, r etcd.OpResponse) int64 {
			return r.Get().Count
		},
	)
}

func (v Prefix) GetAll(opts ...etcd.OpOption) GetManyOp {
	return NewGetManyOp(
		func(_ context.Context) (*etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
			etcdOp := etcd.OpGet(v.Prefix(), opts...)
			return &etcdOp, nil
		}, func(_ context.Context, r etcd.OpResponse) ([]*KeyValue, error) {
			return r.Get().Kvs, nil
		},
	)
}

func (v Prefix) DeleteAll(opts ...etcd.OpOption) CountOp {
	return NewCountOp(
		func(_ context.Context) (*etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
			etcdOp := etcd.OpDelete(v.Prefix(), opts...)
			return &etcdOp, nil
		},
		func(_ context.Context, r etcd.OpResponse) int64 {
			return r.Del().Deleted
		},
	)
}

func (v PrefixT[T]) GetAll(opts ...etcd.OpOption) GetManyTOp[T] {
	return NewGetManyTOp(
		func(_ context.Context) (*etcd.Op, error) {
			opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
			etcdOp := etcd.OpGet(v.Prefix(), opts...)
			return &etcdOp, nil
		},
		func(ctx context.Context, r etcd.OpResponse) (KeyValuesT[T], error) {
			kvs := r.Get().Kvs
			out := make([]KeyValueT[T], len(kvs))
			for i, kv := range kvs {
				out[i].KV = kv
				if err := v.serialization.decodeAndValidate(ctx, kv, &out[i].Value); err != nil {
					return nil, invalidKeyError(string(kv.Key), err)
				}
			}
			return out, nil
		},
	)
}
