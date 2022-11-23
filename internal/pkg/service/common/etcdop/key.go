package etcdop

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Key represents an etcd key - one key, not a prefix.
type Key string

func (v Key) Key() string {
	return string(v)
}

func (v Key) Exists(opts ...etcd.OpOption) BoolOp {
	opts = append([]etcd.OpOption{etcd.WithCountOnly()}, opts...)
	return NewBoolOp(etcd.OpGet(v.Key(), opts...), func(r etcd.OpResponse) (bool, error) {
		count := r.Get().Count
		if count == 0 {
			return false, nil
		} else if count == 1 {
			return true, nil
		} else {
			return false, errors.Errorf(`etcd exists: at most one result result expected, found %d results`, count)
		}
	})
}

func (v Key) Get(opts ...etcd.OpOption) GetOneOp {
	return NewGetOneOp(etcd.OpGet(v.Key(), opts...), func(r etcd.OpResponse) (*mvccpb.KeyValue, error) {
		count := r.Get().Count
		if count == 0 {
			return nil, nil
		} else if count == 1 {
			return r.Get().Kvs[0], nil
		} else {
			return nil, errors.Errorf(`etcd get: at most one result result expected, found %d results`, count)
		}
	})
}

func (v Key) Delete(opts ...etcd.OpOption) BoolOp {
	return NewBoolOp(etcd.OpDelete(v.Key(), opts...), func(r etcd.OpResponse) (bool, error) {
		count := r.Del().Deleted
		if count == 0 {
			return false, nil
		} else if count == 1 {
			return true, nil
		} else {
			return false, errors.Errorf(`etcd delete: at most one result result expected, found %d results`, count)
		}
	})
}

func (v Key) Put(val string, opts ...etcd.OpOption) NoResultOp {
	return NewNoResultOp(etcd.OpPut(v.Key(), val, opts...), func(_ etcd.OpResponse) error {
		// response is always OK
		return nil
	})
}

func (v Key) PutIfNotExists(val string, opts ...etcd.OpOption) BoolOp {
	op := etcd.OpTxn(
		[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
		[]etcd.Op{etcd.OpPut(v.Key(), val, opts...)},
		[]etcd.Op{},
	)
	return NewBoolOp(op, func(r etcd.OpResponse) (bool, error) {
		return r.Txn().Succeeded, nil
	})
}
