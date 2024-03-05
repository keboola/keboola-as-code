package appconfig

import (
	"sync"
)

// SafeMap is a map wrapper with RWMutex for safe concurrent access.
type SafeMap[K comparable, V any] struct {
	*sync.RWMutex
	m map[K]V
}

func NewSafeMap[K comparable, V any]() SafeMap[K, V] {
	return SafeMap[K, V]{
		RWMutex: &sync.RWMutex{},
		m:       make(map[K]V),
	}
}

func (m SafeMap[K, V]) Get(key K) (V, bool) {
	m.RLock()
	defer m.RUnlock()
	item, ok := m.m[key]
	return item, ok
}

func (m SafeMap[K, V]) Set(key K, val V) {
	m.Lock()
	defer m.Unlock()
	m.m[key] = val
}
