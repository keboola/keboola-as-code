package writer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Events provides listening to the writer lifecycle.
type Events struct {
	parent        *Events
	onWriterOpen  []func(w Writer) error
	onWriterClose []func(w Writer, closeErr error) error
}

func NewEvents() *Events {
	return &Events{}
}

// OnWriterOpen registers a callback that is invoked in the LIFO order when a new writer is created.
func (e *Events) OnWriterOpen(fn func(Writer) error) {
	e.onWriterOpen = append(e.onWriterOpen, fn)
}

// OnWriterClose registers a callback that is invoked in the LIFO order when a writer is closed.
func (e *Events) OnWriterClose(fn func(Writer, error) error) {
	e.onWriterClose = append(e.onWriterClose, fn)
}

// Clone returns a new independent instance of the Events with reference to the parent value.
func (e *Events) Clone() *Events {
	return &Events{parent: e}
}

func (e *Events) dispatchOnWriterOpen(w Writer) error {
	errs := errors.NewMultiError()

	// Invoke listeners in the LIFO order
	node := e
	for node != nil {
		listeners := node.onWriterOpen
		for i := len(listeners) - 1; i >= 0; i-- {
			if err := listeners[i](w); err != nil {
				errs.Append(err)
			}
		}

		node = node.parent
	}

	return errs.ErrorOrNil()
}

func (e *Events) dispatchOnWriterClose(w Writer, closeErr error) error {
	errs := errors.NewMultiError()

	// Invoke listeners in the LIFO order
	node := e
	for node != nil {
		listeners := node.onWriterClose
		for i := len(listeners) - 1; i >= 0; i-- {
			if err := listeners[i](w, closeErr); err != nil {
				errs.Append(err)
			}
		}

		node = node.parent
	}

	return errs.ErrorOrNil()
}
