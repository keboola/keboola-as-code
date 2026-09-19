package syncmap

import (
	"sync"
)

// SyncMap is a map wrapper with RWMutex for safe concurrent access.
type SyncMap[K comparable, V any] struct {
	init func(K) *V
	lock *sync.Mutex
	kvs  map[K]*V
}

func New[K comparable, V any](init func(K) *V) *SyncMap[K, V] {
	return &SyncMap[K, V]{
		init: init,
		lock: &sync.Mutex{},
		kvs:  make(map[K]*V),
	}
}

func (m *SyncMap[K, V]) GetOrInit(key K) *V {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Get
	item, ok := m.kvs[key]

	// Or init
	if !ok {
		item = m.init(key)
		m.kvs[key] = item
	}

	return item
}

// Delete removes the key and returns the item it held, so the caller can
// release whatever that item owns. Without it a map keyed by a short-lived
// identity grows for the life of the process.
func (m *SyncMap[K, V]) Delete(key K) (*V, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	item, ok := m.kvs[key]
	if ok {
		delete(m.kvs, key)
	}
	return item, ok
}
