package etcdop

import (
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

const (
	CreateEvent EventType = iota
	UpdateEvent
	DeleteEvent
)

type WatchEvent[T any] struct {
	Type   EventType
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Key    string
	Value  T
	// PrevValue is set only for UpdateEvent if etcd.WithPrevKV() option is used.
	PrevValue *T
}

type EventType int

// WatchResponseRaw for untyped prefix.
type WatchResponseRaw = WatchResponseE[WatchEvent[[]byte]]

// WatchResponseE wraps events of the type E together with WatcherStatus.
type WatchResponseE[E any] struct {
	WatcherStatus
	Events []E
}

func (v EventType) String() string {
	switch v {
	case CreateEvent:
		return "create"
	case UpdateEvent:
		return "update"
	case DeleteEvent:
		return "delete"
	default:
		return strconv.Itoa(int(v))
	}
}

func (e *WatchResponseE[E]) Rev() int64 {
	return e.Header.Revision
}
