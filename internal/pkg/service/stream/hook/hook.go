// Package hook provides registry of various hooks, from different parts of the system.
// Hooks provide loose coupling between system parts.
package hook

// Registry part is used to register hooks callbacks.
type Registry struct {
	source               fnList[sourceHook]
	sink                 fnList[sinkHook]
	fileStateTransition  fnList[fileStateTransitionHook]
	sliceStateTransition fnList[sliceStateTransitionHook]
	fileDelete           fnList[fileDeleteHook]
	sliceDelete          fnList[sliceDeleteHook]
}

// Executor part is used to invoke hooks callbacks.
type Executor struct {
	hooks *Registry
}

type fnList[T any] []T

func New() (*Registry, *Executor) {
	h := &Registry{}
	e := &Executor{hooks: h}
	return h, e
}

func (fns fnList[T]) forEach(do func(T)) {
	for _, fn := range fns {
		do(fn)
	}
}
