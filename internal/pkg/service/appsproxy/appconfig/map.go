package appconfig

import (
	"sync"
)

// SafeMap is a map wrapper with RWMutex for safe concurrent access.
type SafeMap[K comparable, V any] struct {
	lock sync.RWMutex
	m    map[K]V
}

func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		lock: sync.RWMutex{},
		m:       make(map[K]V),
	}
}

func (m *SafeMap[K, V]) Get(key K) (V, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	item, ok := m.m[key]
	return item, ok
}

func (m *SafeMap[K, V]) Set(key K, val V) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.m[key] = val
}
