package appconfig

import (
	"sync"
)

// SafeMap is a map wrapper with RWMutex for safe concurrent access.
type SafeMap[K comparable, V any] struct {
	init func() *V
	lock *sync.Mutex
	kvs  map[K]*V
}

func NewSafeMap[K comparable, V any](init func() *V) *SafeMap[K, V] {
	return &SafeMap[K, V]{
		init: init,
		lock: &sync.Mutex{},
		kvs:  make(map[K]*V),
	}
}

func (m *SafeMap[K, V]) GetOrInit(key K) *V {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Get
	item, ok := m.kvs[key]

	// Or init
	if !ok {
		item = m.init()
		m.kvs[key] = item
	}

	return item
}
