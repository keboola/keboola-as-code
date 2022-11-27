package etcdop

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Key represents an etcd key - one key, not a prefix.
type Key string

type key = Key

// KeyT extends Key with generic functionality, contains type of the serialized value.
type KeyT[T any] struct {
	key
	serialization Serialization
}

func (v Key) Key() string {
	return string(v)
}

func (v Key) Exists(opts ...etcd.OpOption) BoolOp {
	opts = append([]etcd.OpOption{etcd.WithCountOnly()}, opts...)
	return NewBoolOp(
		func(_ context.Context) (*etcd.Op, error) {
			etcdOp := etcd.OpGet(v.Key(), opts...)
			return &etcdOp, nil
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

func (v Key) Get(opts ...etcd.OpOption) GetOneOp {
	return NewGetOneOp(
		func(_ context.Context) (*etcd.Op, error) {
			etcdOp := etcd.OpGet(v.Key(), opts...)
			return &etcdOp, nil
		},
		func(_ context.Context, r etcd.OpResponse) (*KeyValue, error) {
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

func (v Key) Delete(opts ...etcd.OpOption) BoolOp {
	return NewBoolOp(
		func(_ context.Context) (*etcd.Op, error) {
			etcdOp := etcd.OpDelete(v.Key(), opts...)
			return &etcdOp, nil
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

func (v Key) Put(val string, opts ...etcd.OpOption) NoResultOp {
	return NewNoResultOp(
		func(_ context.Context) (*etcd.Op, error) {
			etcdOp := etcd.OpPut(v.Key(), val, opts...)
			return &etcdOp, nil
		},
		func(_ context.Context, _ etcd.OpResponse) error {
			// response is always OK
			return nil
		},
	)
}

func (v Key) PutIfNotExists(val string, opts ...etcd.OpOption) BoolOp {
	return NewBoolOp(
		func(_ context.Context) (*etcd.Op, error) {
			etcdOp := etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
				[]etcd.Op{etcd.OpPut(v.Key(), val, opts...)},
				[]etcd.Op{},
			)
			return &etcdOp, nil
		},
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			return r.Txn().Succeeded, nil
		},
	)
}

func (v KeyT[T]) Get(opts ...etcd.OpOption) GetOneTOp[T] {
	return NewGetOneTOp(
		func(_ context.Context) (*etcd.Op, error) {
			etcdOp := etcd.OpGet(v.Key(), opts...)
			return &etcdOp, nil
		},
		func(ctx context.Context, r etcd.OpResponse) (*KeyValueT[T], error) {
			count := r.Get().Count
			if count == 0 {
				return nil, nil
			} else if count == 1 {
				kv := r.Get().Kvs[0]
				target := new(T)
				if err := v.serialization.decodeAndValidate(ctx, kv, target); err != nil {
					return nil, invalidKeyError(string(kv.Key), err)
				}
				return &KeyValueT[T]{Value: *target, KV: kv}, nil
			} else {
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v KeyT[T]) Put(val T, opts ...etcd.OpOption) NoResultOp {
	return NewNoResultOp(
		func(ctx context.Context) (*etcd.Op, error) {
			encoded, err := v.serialization.validateAndEncode(ctx, &val)
			if err != nil {
				return nil, invalidKeyError(v.Key(), err)
			}
			etcdOp := etcd.OpPut(v.Key(), encoded, opts...)
			return &etcdOp, nil
		},
		func(_ context.Context, _ etcd.OpResponse) error {
			// response is always OK
			return nil
		},
	)
}

func (v KeyT[T]) PutIfNotExists(val T, opts ...etcd.OpOption) BoolOp {
	return NewBoolOp(
		func(ctx context.Context) (*etcd.Op, error) {
			encoded, err := v.serialization.validateAndEncode(ctx, &val)
			if err != nil {
				return nil, invalidKeyError(v.Key(), err)
			}
			etcdOp := etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
				[]etcd.Op{etcd.OpPut(v.Key(), encoded, opts...)},
				[]etcd.Op{},
			)
			return &etcdOp, nil
		},
		func(_ context.Context, r etcd.OpResponse) (bool, error) {
			return r.Txn().Succeeded, nil
		},
	)
}

func invalidKeyError(key string, err error) error {
	return errors.Errorf(`invalid etcd key "%s": %w`, key, err)
}
