// Package prefixtree wraps go-radix library, adds lock and generic type support.
package prefixtree

import (
	"sync"

	"github.com/armon/go-radix"
)

type AtomicTree[T any] struct {
	lock *sync.RWMutex
	tree *Tree[T]
}

type Tree[T any] struct {
	tree *radix.Tree
}

type TreeReadOnly[T any] interface {
	Get(key string) (T, bool)
	AllFromPrefix(key string) []T
	FirstFromPrefix(key string) (value T, found bool)
	LastFromPrefix(key string) (value T, found bool)
	WalkAll(fn func(key string, value T) (stop bool))
	WalkPrefix(key string, fn func(key string, value T) (stop bool))
	ToMap() map[string]T
}

func New[T any]() *AtomicTree[T] {
	return NewWithLock[T](&sync.RWMutex{})
}

func NewWithLock[T any](lock *sync.RWMutex) *AtomicTree[T] {
	return &AtomicTree[T]{
		lock: lock,
		tree: &Tree[T]{tree: radix.New()},
	}
}

// Atomic can be used to make multiple atomic changes, under an exclusive lock.
func (t *AtomicTree[T]) Atomic(do func(t *Tree[T])) {
	t.lock.Lock()
	defer t.lock.Unlock()
	do(t.tree)
}

// AtomicReadOnly can be used to make multiple atomic read operations.
func (t *AtomicTree[T]) AtomicReadOnly(do func(t TreeReadOnly[T])) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	do(t.tree)
}

func (t *AtomicTree[T]) Len() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.Len()
}

func (t *AtomicTree[T]) All() []T {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.All()
}

func (t *AtomicTree[T]) AllFromPrefix(key string) []T {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.AllFromPrefix(key)
}

func (t *AtomicTree[T]) FirstFromPrefix(key string) (value T, found bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.FirstFromPrefix(key)
}

func (t *AtomicTree[T]) LastFromPrefix(key string) (value T, found bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.LastFromPrefix(key)
}

func (t *AtomicTree[T]) Insert(key string, value T) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.tree.Insert(key, value)
}

func (t *AtomicTree[T]) Delete(key string) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.tree.Delete(key)
}

func (t *AtomicTree[T]) Get(key string) (T, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.Get(key)
}

func (t *AtomicTree[T]) Reset() {
	t.lock.RLock()
	defer t.lock.RUnlock()
	t.tree.Reset()
}

func (t *AtomicTree[T]) WalkPrefix(key string, fn func(key string, value T) (stop bool)) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	t.tree.WalkPrefix(key, fn)
}

func (t *AtomicTree[T]) WalkAll(fn func(key string, value T) (stop bool)) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	t.tree.WalkAll(fn)
}

func (t *AtomicTree[T]) ToMap() map[string]T {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.tree.ToMap()
}

func (t *AtomicTree[T]) DeletePrefix(key string) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	t.tree.DeletePrefix(key)
}

func (t *Tree[T]) Len() int {
	return t.tree.Len()
}

func (t *Tree[T]) All() []T {
	var out []T
	t.WalkAll(func(_ string, value T) bool {
		out = append(out, value)
		return false
	})
	return out
}

func (t *Tree[T]) AllFromPrefix(key string) []T {
	var out []T
	t.WalkPrefix(key, func(_ string, value T) bool {
		out = append(out, value)
		return false
	})
	return out
}

func (t *Tree[T]) FirstFromPrefix(key string) (value T, found bool) {
	t.WalkPrefix(key, func(_ string, v T) bool {
		value = v
		found = true
		return true
	})
	return value, found
}

func (t *Tree[T]) LastFromPrefix(key string) (value T, found bool) {
	t.WalkPrefix(key, func(_ string, v T) bool {
		value = v
		found = true
		return false
	})
	return value, found
}

func (t *Tree[T]) Insert(key string, value T) {
	t.tree.Insert(key, value)
}

func (t *Tree[T]) Delete(key string) bool {
	_, ok := t.tree.Delete(key)
	return ok
}

func (t *Tree[T]) Get(key string) (T, bool) {
	val, found := t.tree.Get(key)
	if !found {
		var empty T
		return empty, false
	}
	return val.(T), true
}

func (t *Tree[T]) Reset() {
	var keys []string
	t.WalkPrefix("", func(key string, _ T) bool {
		keys = append(keys, key)
		return false
	})
	// Keys must be deleted outside WalkPrefix
	for _, key := range keys {
		t.Delete(key)
	}
}

func (t *Tree[T]) WalkPrefix(key string, fn func(key string, value T) (stop bool)) {
	t.tree.WalkPrefix(key, func(key string, value any) bool {
		return fn(key, value.(T))
	})
}

func (t *Tree[T]) WalkAll(fn func(key string, value T) (stop bool)) {
	t.tree.WalkPrefix("", func(key string, value any) bool {
		return fn(key, value.(T))
	})
}

func (t *Tree[T]) ToMap() map[string]T {
	m := make(map[string]T)
	t.WalkPrefix("", func(key string, value T) bool {
		m[key] = value
		return false
	})
	return m
}

func (t *Tree[T]) DeletePrefix(key string) {
	var toDelete []string
	t.WalkPrefix(key, func(key string, _ T) bool {
		toDelete = append(toDelete, key)
		return false
	})
	for _, k := range toDelete {
		t.Delete(k)
	}
}
