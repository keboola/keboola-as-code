package dependencies

import (
	"sync"
)

// Lazy helper allows lazy initialization of a value on the first use.
// Initialization runs only once, other calls wait.
type Lazy[T any] struct {
	lock  sync.Mutex
	value *T
}

func (s Lazy[T]) Set(v T) {
	s.value = &v
}

func (s Lazy[T]) InitAndGet(initFn func() (*T, error)) (T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Is already initialized?
	if s.value != nil {
		return *s.value, nil
	}

	// Initialize
	if v, err := initFn(); err == nil {
		s.value = v
		return *v, nil
	} else {
		return *new(T), err
	}
}
