package etcdop

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Key represents an etcd key - one key, not a prefix.
type Key string

type key = Key

// KeyT extends Key with generic functionality, contains type of the serialized value.
type KeyT[T any] struct {
	key
	serde *serde.Serde
}

func (v Key) Key() string {
	return string(v)
}

func (v Key) Exists(client etcd.KV, opts ...etcd.OpOption) op.BoolOp {
	opts = append([]etcd.OpOption{etcd.WithCountOnly()}, opts...)
	return op.NewBoolOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(_ context.Context, raw op.RawResponse) (bool, error) {
			count := raw.Get().Count
			if count == 0 {
				return false, nil
			} else if count == 1 {
				return true, nil
			} else {
				return false, errors.Errorf(`etcd exists: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v Key) Get(client etcd.KV, opts ...etcd.OpOption) op.GetOneOp {
	return op.NewGetOneOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(_ context.Context, raw op.RawResponse) (*op.KeyValue, error) {
			count := raw.Get().Count
			if count == 0 {
				return nil, nil
			} else if count == 1 {
				return raw.Get().Kvs[0], nil
			} else {
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v Key) Delete(client etcd.KV, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpDelete(v.Key(), opts...), nil
		},
		func(_ context.Context, raw op.RawResponse) (bool, error) {
			count := raw.Del().Deleted
			if count == 0 {
				return false, nil
			} else if count == 1 {
				return true, nil
			} else {
				return false, errors.Errorf(`etcd delete: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v Key) DeleteIfExists(client etcd.KV, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "!=", 0)},
				[]etcd.Op{etcd.OpDelete(v.Key(), opts...)},
				[]etcd.Op{},
			), nil
		},
		func(_ context.Context, raw op.RawResponse) (bool, error) {
			return raw.Txn().Succeeded, nil
		},
	)
}

func (v Key) Put(client etcd.KV, val string, opts ...etcd.OpOption) op.NoResultOp {
	return op.NewNoResultOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpPut(v.Key(), val, opts...), nil
		},
		func(_ context.Context, _ op.RawResponse) error {
			// response is always OK
			return nil
		},
	)
}

func (v Key) PutIfNotExists(client etcd.KV, val string, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
				[]etcd.Op{etcd.OpPut(v.Key(), val, opts...)},
				[]etcd.Op{},
			), nil
		},
		func(_ context.Context, raw op.RawResponse) (bool, error) {
			return raw.Txn().Succeeded, nil
		},
	)
}

func (v KeyT[T]) Get(client etcd.KV, opts ...etcd.OpOption) op.ForType[*op.KeyValueT[T]] {
	return op.NewGetOneTOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, raw op.RawResponse) (*op.KeyValueT[T], error) {
			count := raw.Get().Count
			if count == 0 {
				return nil, nil
			} else if count == 1 {
				kv := raw.Get().Kvs[0]
				target := new(T)
				if err := v.serde.Decode(ctx, kv, target); err != nil {
					return nil, errors.Errorf("etcd operation \"get\" failed: %w", invalidValueError(v.Key(), err))
				}
				return &op.KeyValueT[T]{Value: *target, Kv: kv}, nil
			} else {
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v KeyT[T]) GetValue(client etcd.KV, opts ...etcd.OpOption) op.ForType[T] {
	return op.NewForType(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, raw op.RawResponse) (T, error) {
			var target T
			count := raw.Get().Count
			if count == 0 {
				return target, op.NewEmptyResultError(errors.Errorf(`key "%s" not found`, v.Key()))
			} else if count == 1 {
				kv := raw.Get().Kvs[0]
				if err := v.serde.Decode(ctx, kv, &target); err != nil {
					return target, errors.Errorf("etcd operation \"get\" failed: %w", invalidValueError(v.Key(), err))
				}
				return target, nil
			} else {
				return target, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v KeyT[T]) Put(client etcd.KV, val T, opts ...etcd.OpOption) op.ForType[T] {
	return op.NewForType[T](
		client,
		func(ctx context.Context) (etcd.Op, error) {
			encoded, err := v.serde.Encode(ctx, &val)
			if err != nil {
				return etcd.Op{}, errors.Errorf("etcd operation \"put\" failed: %w", invalidValueError(v.Key(), err))
			}
			return etcd.OpPut(v.Key(), encoded, opts...), nil
		},
		func(_ context.Context, _ op.RawResponse) (T, error) {
			// Result is inserted value
			return val, nil
		},
	)
}

func (v KeyT[T]) PutIfNotExists(client etcd.KV, val T, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		client,
		func(ctx context.Context) (etcd.Op, error) {
			encoded, err := v.serde.Encode(ctx, &val)
			if err != nil {
				return etcd.Op{}, errors.Errorf("etcd operation \"put if not exists\" failed: %w", invalidValueError(v.Key(), err))
			}
			return etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
				[]etcd.Op{etcd.OpPut(v.Key(), encoded, opts...)},
				[]etcd.Op{},
			), nil
		},
		func(_ context.Context, raw op.RawResponse) (bool, error) {
			return raw.Txn().Succeeded, nil
		},
	)
}

func invalidValueError(key string, err error) error {
	return errors.PrefixErrorf(err, `invalid value for "%s"`, key)
}
