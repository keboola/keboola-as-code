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
	serde serde.Serde
}

func (v Key) Key() string {
	return string(v)
}

func (v Key) Exists(opts ...etcd.OpOption) op.BoolOp {
	opts = append([]etcd.OpOption{etcd.WithCountOnly()}, opts...)
	return op.NewBoolOp(
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			count := r.Get().Count
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

func (v Key) Get(opts ...etcd.OpOption) op.GetOneOp {
	return op.NewGetOneOp(
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(_ context.Context, r etcd.OpResponse) (*op.KeyValue, error) {
			count := r.Get().Count
			if count == 0 {
				return nil, nil
			} else if count == 1 {
				return r.Get().Kvs[0], nil
			} else {
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v Key) Delete(opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpDelete(v.Key(), opts...), nil
		},
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			count := r.Del().Deleted
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

func (v Key) Put(val string, opts ...etcd.OpOption) op.NoResultOp {
	return op.NewNoResultOp(
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpPut(v.Key(), val, opts...), nil
		},
		func(_ context.Context, _ etcd.OpResponse) error {
			// response is always OK
			return nil
		},
	)
}

func (v Key) PutIfNotExists(val string, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
				[]etcd.Op{etcd.OpPut(v.Key(), val, opts...)},
				[]etcd.Op{},
			), nil
		},
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			return r.Txn().Succeeded, nil
		},
	)
}

func (v KeyT[T]) Get(opts ...etcd.OpOption) op.ForType[*op.KeyValueT[T]] {
	return op.NewGetOneTOp(
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, r etcd.OpResponse) (*op.KeyValueT[T], error) {
			count := r.Get().Count
			if count == 0 {
				return nil, nil
			} else if count == 1 {
				kv := r.Get().Kvs[0]
				target := new(T)
				if err := v.serde.Decode(ctx, kv, target); err != nil {
					return nil, errors.Errorf("etcd operation \"get one\" failed: %w", invalidValueError(v.Key(), err))
				}
				return &op.KeyValueT[T]{Value: *target, KV: kv}, nil
			} else {
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v KeyT[T]) Put(val T, opts ...etcd.OpOption) op.NoResultOp {
	return op.NewNoResultOp(
		func(ctx context.Context) (etcd.Op, error) {
			encoded, err := v.serde.Encode(ctx, &val)
			if err != nil {
				return etcd.Op{}, errors.Errorf("etcd operation \"put\" failed: %w", invalidValueError(v.Key(), err))
			}
			return etcd.OpPut(v.Key(), encoded, opts...), nil
		},
		func(_ context.Context, _ etcd.OpResponse) error {
			// response is always OK
			return nil
		},
	)
}

func (v KeyT[T]) PutIfNotExists(val T, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
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
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			return r.Txn().Succeeded, nil
		},
	)
}

func invalidValueError(key string, err error) error {
	return errors.Errorf(`invalid value for "%s": %w`, key, err)
}
