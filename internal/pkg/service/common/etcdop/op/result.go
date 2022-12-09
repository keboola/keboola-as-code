package op

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// KeyValue - operation result.
type KeyValue = mvccpb.KeyValue

type KeyValuesT[T any] []KeyValueT[T]

// KeyValueT - typed operation result.
type KeyValueT[T any] struct {
	Value T
	KV    *KeyValue
}

func (kv KeyValueT[T]) Key() string {
	return string(kv.KV.Key)
}

func (kvs KeyValuesT[T]) Values() (out []T) {
	out = make([]T, len(kvs))
	for i, kv := range kvs {
		out[i] = kv.Value
	}
	return out
}
