// Package events providers listening/dispatching of the open and close events for writers/readers/pipelines.
// Events callbacks can be attached on different levels, e.g. on all Volumes, on one Volume, on one Writer, see Events.Clone method.
package events

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Events provides listening to the writer lifecycle.
type Events[T any] struct {
	parent  *Events[T]
	onOpen  []func(t T) error
	onClose []func(t T, closeErr error) error
}

func New[T any]() *Events[T] {
	return &Events[T]{}
}

// Clone returns a new independent instance of the Events with reference to the parent value.
func (e *Events[T]) Clone() *Events[T] {
	clone := New[T]()
	clone.parent = e
	return clone
}

// OnOpen registers a callback that is invoked in the LIFO order when a new writer is created.
func (e *Events[T]) OnOpen(fn func(T) error) {
	e.onOpen = append(e.onOpen, fn)
}

// OnClose registers a callback that is invoked in the LIFO order when a writer is closed.
func (e *Events[T]) OnClose(fn func(T, error) error) {
	e.onClose = append(e.onClose, fn)
}

func (e *Events[T]) DispatchOnOpen(t T) error {
	errs := errors.NewMultiError()

	// Invoke listeners in the LIFO order
	node := e
	for node != nil {
		listeners := node.onOpen
		for i := len(listeners) - 1; i >= 0; i-- {
			if err := listeners[i](t); err != nil {
				errs.Append(err)
			}
		}

		node = node.parent
	}

	return errs.ErrorOrNil()
}

func (e *Events[T]) DispatchOnClose(t T, closeErr error) error {
	errs := errors.NewMultiError()

	// Invoke listeners in the LIFO order
	node := e
	for node != nil {
		listeners := node.onClose
		for i := len(listeners) - 1; i >= 0; i-- {
			if err := listeners[i](t, closeErr); err != nil {
				errs.Append(err)
			}
		}

		node = node.parent
	}

	return errs.ErrorOrNil()
}
