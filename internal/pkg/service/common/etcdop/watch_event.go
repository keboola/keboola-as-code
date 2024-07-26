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

// WatchEvent is interface for WatchEventRaw and WatchEventT.
type WatchEvent interface {
	EventType() EventType
	// HasPrevValue returns true, if the event contains previous value of the key.
	HasPrevValue() bool
}

type WatchEventRaw struct {
	Type   EventType
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
}

type EventType int

// WatchResponseRaw for untyped prefix.
type WatchResponseRaw = WatchResponseE[WatchEventRaw]

// WatchResponseE wraps events of the type E together with WatcherStatus.
type WatchResponseE[E any] struct {
	WatcherStatus
	Events []E
}

func (e WatchEventRaw) EventType() EventType {
	return e.Type
}

func (e WatchEventRaw) HasPrevValue() bool {
	return e.PrevKv != nil
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
