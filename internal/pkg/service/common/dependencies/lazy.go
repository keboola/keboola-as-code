package dependencies

import (
	"github.com/sasha-s/go-deadlock"
)

// Lazy helper allows lazy initialization of a value on the first use.
// Initialization runs only once, other calls wait.
type Lazy[T any] struct {
	lock  deadlock.Mutex
	value T
	set   bool
}

func (s *Lazy[T]) IsSet() bool {
	return s.set
}

func (s *Lazy[T]) Set(v T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.value = v
	s.set = true
}

func (s *Lazy[T]) MustInitAndGet(initFn func() T) T {
	v, err := s.InitAndGet(func() (T, error) {
		return initFn(), nil
	})
	if err != nil {
		panic(err)
	}
	return v
}

func (s *Lazy[T]) InitAndGet(initFn func() (T, error)) (T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Is already initialized?
	if s.set {
		return s.value, nil
	}

	// Initialize
	if v, err := initFn(); err == nil {
		s.value = v
		s.set = true
		return v, nil
	} else {
		return v, err
	}
}
