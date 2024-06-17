package etcdop

import (
	"context"
	"regexp"

	"github.com/umisama/go-regexpcache"
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

func NewKey(v string) Key {
	return Key(v)
}

func NewTypedKey[T any](v string, s *serde.Serde) KeyT[T] {
	return KeyT[T]{key: NewKey(v), serde: s}
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
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
			count := raw.Get().Count
			switch count {
			case 0:
				return false, nil
			case 1:
				return true, nil
			default:
				return false, errors.Errorf(`etcd exists: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v Key) Get(client etcd.KV, opts ...etcd.OpOption) op.WithResult[*op.KeyValue] {
	return op.NewForType(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(_ context.Context, raw *op.RawResponse) (*op.KeyValue, error) {
			count := raw.Get().Count
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

func (v Key) Delete(client etcd.KV, opts ...etcd.OpOption) op.BoolOp {
	return op.NewBoolOp(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpDelete(v.Key(), opts...), nil
		},
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
			count := raw.Del().Deleted
			switch count {
			case 0:
				return false, nil
			case 1:
				return true, nil
			default:
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
				nil,
			), nil
		},
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
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
		func(_ context.Context, _ *op.RawResponse) error {
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
				nil,
			), nil
		},
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
			return raw.Txn().Succeeded, nil
		},
	)
}

func (v KeyT[T]) ReplacePrefix(old, repl string) KeyT[T] {
	v.key = Key(regexpcache.MustCompile("^"+regexp.QuoteMeta(old)).ReplaceAllString(string(v.key), repl))
	return v
}

// GetKV gets a decoded value of the key wrapped with metadata to op.KeyValueT.
// If the key is missing, result is nil pointer.
func (v KeyT[T]) GetKV(client etcd.KV, opts ...etcd.OpOption) op.WithResult[*op.KeyValueT[T]] {
	return op.NewForType(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, raw *op.RawResponse) (*op.KeyValueT[T], error) {
			count := raw.Get().Count
			switch count {
			case 0:
				return nil, nil
			case 1:
				kv := raw.Get().Kvs[0]
				target := new(T)
				if err := v.serde.Decode(ctx, kv, target); err != nil {
					return nil, errors.Errorf("etcd operation \"get\" failed: %w", invalidValueError(v.Key(), err))
				}
				return &op.KeyValueT[T]{Value: *target, Kv: kv}, nil
			default:
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

// GetOrNil gets a decoded value of the key as pointer.
// If the key is missing, result is nil pointer.
func (v KeyT[T]) GetOrNil(client etcd.KV, opts ...etcd.OpOption) op.WithResult[*T] {
	return op.NewForType(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, raw *op.RawResponse) (*T, error) {
			switch count := raw.Get().Count; count {
			case 0:
				// Return nil pointer if the key is missing
				return nil, nil
			case 1:
				target := new(T)
				kv := raw.Get().Kvs[0]
				if err := v.serde.Decode(ctx, kv, target); err != nil {
					return target, errors.Errorf("etcd operation \"get\" failed: %w", invalidValueError(v.Key(), err))
				}
				return target, nil
			default:
				return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

// GetOrErr gets a decoded value of the key.
// If the key is missing, an error is returned.
func (v KeyT[T]) GetOrErr(client etcd.KV, opts ...etcd.OpOption) op.WithResult[T] {
	return op.NewForType(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, raw *op.RawResponse) (T, error) {
			var target T
			switch count := raw.Get().Count; count {
			case 0:
				return target, op.NewEmptyResultError(errors.Errorf(`key "%s" not found`, v.Key()))
			case 1:
				kv := raw.Get().Kvs[0]
				if err := v.serde.Decode(ctx, kv, &target); err != nil {
					return target, errors.Errorf("etcd operation \"get\" failed: %w", invalidValueError(v.Key(), err))
				}
				return target, nil
			default:
				return target, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

// GetOrEmpty gets a decoded value of the key.
// If the key is missing, the empty value is returned.
func (v KeyT[T]) GetOrEmpty(client etcd.KV, opts ...etcd.OpOption) op.WithResult[T] {
	return op.NewForType(
		client,
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpGet(v.Key(), opts...), nil
		},
		func(ctx context.Context, raw *op.RawResponse) (T, error) {
			var target T
			switch count := raw.Get().Count; count {
			case 0:
				// Return empty value without result
				return target, nil
			case 1:
				kv := raw.Get().Kvs[0]
				if err := v.serde.Decode(ctx, kv, &target); err != nil {
					return target, errors.Errorf("etcd operation \"get\" failed: %w", invalidValueError(v.Key(), err))
				}
				return target, nil
			default:
				return target, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
			}
		},
	)
}

func (v KeyT[T]) Put(client etcd.KV, val T, opts ...etcd.OpOption) op.WithResult[T] {
	return op.NewForType[T](
		client,
		func(ctx context.Context) (etcd.Op, error) {
			encoded, err := v.serde.Encode(ctx, &val)
			if err != nil {
				return etcd.Op{}, errors.Errorf("etcd operation \"put\" failed: %w", invalidValueError(v.Key(), err))
			}
			return etcd.OpPut(v.Key(), encoded, opts...), nil
		},
		func(_ context.Context, _ *op.RawResponse) (T, error) {
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
				nil,
			), nil
		},
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
			return raw.Txn().Succeeded, nil
		},
	)
}

func invalidValueError(key string, err error) error {
	return errors.PrefixErrorf(err, `invalid value for "%s"`, key)
}
