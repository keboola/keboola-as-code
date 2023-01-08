// Package prefixtree wraps go-radix library, adds lock and generic type support.
package prefixtree

import (
	"sync"

	"github.com/armon/go-radix"
)

type TreeThreadSafe[T any] struct {
	lock *sync.RWMutex
	*Tree[T]
}

type Tree[T any] struct {
	tree *radix.Tree
}

func New[T any]() *TreeThreadSafe[T] {
	return NewWithLock[T](&sync.RWMutex{})
}

func NewWithLock[T any](lock *sync.RWMutex) *TreeThreadSafe[T] {
	return &TreeThreadSafe[T]{
		lock: lock,
		Tree: &Tree[T]{tree: radix.New()},
	}
}

// ModifyAtomic can be used to make multiple atomic changes, under an exclusive lock.
func (t *TreeThreadSafe[T]) ModifyAtomic(do func(t *Tree[T])) {
	t.lock.Lock()
	defer t.lock.Unlock()
	do(t.Tree)
}

func (t *TreeThreadSafe[T]) AllFromPrefix(key string) []T {
	var out []T
	t.WalkPrefix(key, func(_ string, value T) bool {
		out = append(out, value)
		return false
	})
	return out
}

func (t *TreeThreadSafe[T]) FirstFromPrefix(key string) (value T, found bool) {
	t.WalkPrefix(key, func(_ string, v T) bool {
		value = v
		found = true
		return true
	})
	return
}

func (t *TreeThreadSafe[T]) LastFromPrefix(key string) (value T, found bool) {
	t.WalkPrefix(key, func(_ string, v T) bool {
		value = v
		found = true
		return false
	})
	return
}

func (t *TreeThreadSafe[T]) Insert(key string, value T) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.Tree.Insert(key, value)
}

func (t *TreeThreadSafe[T]) Delete(key string) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.Tree.Delete(key)
}

func (t *TreeThreadSafe[T]) Get(key string) (T, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.Tree.Get(key)
}

func (t *TreeThreadSafe[T]) WalkPrefix(key string, fn func(key string, value T) (stop bool)) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	t.Tree.WalkPrefix(key, fn)
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

func (t *Tree[T]) WalkPrefix(key string, fn func(key string, value T) (stop bool)) {
	t.tree.WalkPrefix(key, func(key string, value interface{}) bool {
		return fn(key, value.(T))
	})
}
