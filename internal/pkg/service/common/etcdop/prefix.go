package etcdop

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

// Prefix represents an etcd keys prefix - multiple keys prefix, not a one key.
type Prefix string

func (v Prefix) Prefix() string {
	return string(v)
}

func (v Prefix) Add(str string) Prefix {
	return Prefix(v.Prefix() + str + "/")
}

func (v Prefix) Key(key string) Key {
	return Key(v.Prefix() + key)
}

func (v Prefix) AtLeastOneExists(opts ...etcd.OpOption) BoolOp {
	opts = append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCountOnly()}, opts...)
	return NewBoolOp(etcd.OpGet(v.Prefix(), opts...), func(r etcd.OpResponse) (bool, error) {
		return r.Get().Count > 0, nil
	})
}

func (v Prefix) Count(opts ...etcd.OpOption) CountOp {
	opts = append([]etcd.OpOption{etcd.WithCountOnly(), etcd.WithPrefix()}, opts...)
	return NewCountOp(etcd.OpGet(v.Prefix(), opts...), func(r etcd.OpResponse) int64 {
		return r.Get().Count
	})
}

func (v Prefix) GetAll(opts ...etcd.OpOption) GetManyOp {
	opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
	return NewGetManyOp(etcd.OpGet(v.Prefix(), opts...), func(r etcd.OpResponse) ([]*mvccpb.KeyValue, error) {
		return r.Get().Kvs, nil
	})
}

func (v Prefix) DeleteAll(opts ...etcd.OpOption) CountOp {
	opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
	return NewCountOp(etcd.OpDelete(v.Prefix(), opts...), func(r etcd.OpResponse) int64 {
		return r.Del().Deleted
	})
}
