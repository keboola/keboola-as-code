package jsonnet

import (
	"sync"

	"github.com/google/go-jsonnet"
	"github.com/sasha-s/go-deadlock"
)

type VMPool[T any] struct {
	pool *sync.Pool
}

func NewVMPool[T any](factory func(*VM[T]) *jsonnet.VM) *VMPool[T] {
	return &VMPool[T]{
		pool: &sync.Pool{
			New: func() any {
				vm := &VM[T]{
					lock: deadlock.Mutex{},
				}

				vm.vm = factory(vm)
				vm.err = vm.vm.Freeze()

				return vm
			},
		},
	}
}

func (p *VMPool[T]) Get() *VM[T] {
	return p.pool.Get().(*VM[T])
}

func (p *VMPool[T]) Put(vm *VM[T]) {
	p.pool.Put(vm)
}
