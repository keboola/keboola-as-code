// Package prefixtree wraps go-radix library, adds lock and generic type support.
package prefixtree

import (
	"sync"

	"github.com/armon/go-radix"
)

type Tree[T any] struct {
	lock *sync.RWMutex
	tree *radix.Tree
}

func New[T any]() *Tree[T] {
	return NewWithLock[T](&sync.RWMutex{})
}

func NewWithLock[T any](lock *sync.RWMutex) *Tree[T] {
	return &Tree[T]{
		lock: lock,
		tree: radix.New(),
	}
}

func (t *Tree[T]) Insert(key string, value T) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.tree.Insert(key, value)
}

func (t *Tree[T]) Delete(key string) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	_, ok := t.tree.Delete(key)
	return ok
}

func (t *Tree[T]) Get(key string) (T, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	val, found := t.tree.Get(key)
	if !found {
		var empty T
		return empty, false
	}
	return val.(T), true
}

func (t *Tree[T]) WalkPrefix(key string, fn func(key string, value T) (stop bool)) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	t.tree.WalkPrefix(key, func(key string, value interface{}) bool {
		return fn(key, value.(T))
	})
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
	return
}

func (t *Tree[T]) LastFromPrefix(key string) (value T, found bool) {
	t.WalkPrefix(key, func(_ string, v T) bool {
		value = v
		found = true
		return false
	})
	return
}
