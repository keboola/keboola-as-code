package jsonnet

import (
	"sync"

	"github.com/google/go-jsonnet"
)

type Pool[T any] struct {
	pool *sync.Pool
}

func NewPool[T any](factory func(*VM[T]) *jsonnet.VM) *Pool[T] {
	return &Pool[T]{
		pool: &sync.Pool{
			New: func() any {
				vm := &VM[T]{
					lock: sync.Mutex{},
				}

				vm.vm = factory(vm)
				vm.err = vm.vm.Freeze()

				return vm
			},
		},
	}
}

func (p *Pool[T]) Get() *VM[T] {
	return p.pool.Get().(*VM[T])
}

func (p *Pool[T]) Put(vm *VM[T]) {
	p.pool.Put(vm)
}
