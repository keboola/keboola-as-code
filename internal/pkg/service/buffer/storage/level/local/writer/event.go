package writer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type writer = Writer

// EventWriter adds dispatching of the Events for the underlying writer.
type EventWriter struct {
	writer
	events *Events
}

// Events provides listening to the writer lifecycle.
type Events struct {
	parent           *Events
	onWriterCreation []func(w Writer) error
	onWriterClose    []func(w Writer, closeErr error) error
}

// NewEventWriter wraps the Writer to the EventWriter and dispatch "open" event.
func NewEventWriter(w Writer, events *Events) (*EventWriter, error) {
	// Dispatch "open" event
	if err := events.dispatchOnWriterOpen(w); err != nil {
		return nil, err
	}

	// Wrap the Close method
	return &EventWriter{writer: w, events: events}, nil
}

func NewEvents() *Events {
	return &Events{}
}

// Close overwrites the original Close method to dispatch "close" event.
func (w *EventWriter) Close() error {
	errs := errors.NewMultiError()

	cErr := w.writer.Close()
	if cErr != nil {
		errs.Append(cErr)
	}

	if eErr := w.events.dispatchOnWriterClose(w, cErr); eErr != nil {
		errs.Append(eErr)
	}

	return errs.ErrorOrNil()
}

// Unwrap returns underlying writer, used in tests.
func (w *EventWriter) Unwrap() Writer {
	return w.writer
}

// OnWriterOpen registers a callback that is invoked in the LIFO order when a new writer is created.
func (e *Events) OnWriterOpen(fn func(Writer) error) {
	e.onWriterCreation = append(e.onWriterCreation, fn)
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
		listeners := node.onWriterCreation
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
