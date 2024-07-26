package etcdop

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"

type WatchEventT[T any] struct {
	Type   EventType
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Value  T
	// PrevValue is set only for UpdateEvent if etcd.WithPrevKV() option is used.
	PrevValue *T
}

func (e WatchEventT[T]) EventType() EventType {
	return e.Type
}

func (e WatchEventT[T]) HasPrevValue() bool {
	return e.PrevKv != nil
}
