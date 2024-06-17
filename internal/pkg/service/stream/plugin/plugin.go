// Package plugin provides plugin system for loose coupling between layers.
// The advantage is that the code is divided into smaller parts and is easier to understand and test.
package plugin

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

type Plugins struct {
	collection        *Collection
	executor          *Executor
	localStorageSinks []func(sink *definition.Sink) bool
}

type fnList[T any] []T

func New(logger log.Logger) *Plugins {
	c := &Collection{}
	e := &Executor{logger: logger.WithComponent("plugin"), collection: c}
	return &Plugins{
		collection: c,
		executor:   e,
	}
}

func (fns fnList[T]) forEach(do func(T) error) error {
	for _, fn := range fns {
		if err := do(fn); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plugins) Collection() *Collection {
	return p.collection
}

func (p *Plugins) Executor() *Executor {
	return p.executor
}
