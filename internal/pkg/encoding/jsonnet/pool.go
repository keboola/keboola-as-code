package jsonnet

import (
	"sync"

	"github.com/google/go-jsonnet"
)

type vmFactory func(*VM) *jsonnet.VM

type Pool struct {
	pool *sync.Pool
}

func NewPool(factory vmFactory) *Pool {
	return &Pool{
		pool: &sync.Pool{
			New: func() any {
				vm := &VM{
					lock:    sync.Mutex{},
					payload: nil,
				}

				vm.vm = factory(vm)
				vm.err = vm.vm.Freeze()

				return vm
			},
		},
	}
}

func (p *Pool) Get() *VM {
	return p.pool.Get().(*VM)
}

func (p *Pool) Put(vm *VM) {
	p.pool.Put(vm)
}
