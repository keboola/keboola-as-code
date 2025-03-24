package errors

import (
	"github.com/sasha-s/go-deadlock"
)

type MultiError interface {
	Len() int
	ErrorOrNil() error
	Error() string
	WrappedErrors() []error
	Unwrap() error
	StackTrace() StackTrace
	Append(errs ...error)
	AppendNested(err error) NestedError
	AppendWithPrefix(err error, prefix string)
	AppendWithPrefixf(err error, format string, a ...any)
}

type multiErrorGetter interface {
	WrappedErrors() []error
}

type multiError struct {
	lock   *deadlock.Mutex
	trace  StackTrace
	errors []error
}

func NewMultiError() MultiError {
	return &multiError{lock: &deadlock.Mutex{}, trace: callers()}
}

func NewMultiErrorNoTrace() MultiError {
	return &multiError{lock: &deadlock.Mutex{}}
}

func (e *multiError) Error() string {
	return Format(e)
}

func (e *multiError) Len() int {
	if e == nil {
		return 0
	}
	return len(e.errors)
}

func (e *multiError) WrappedErrors() []error {
	return e.errors
}

func (e *multiError) ErrorOrNil() error {
	if e == nil || len(e.errors) == 0 {
		return nil
	}
	e.trace = callers() // store error finalization point
	return e
}

func (e *multiError) Unwrap() error {
	if e == nil || len(e.errors) == 0 {
		return nil
	}
	if len(e.errors) == 1 {
		return e.errors[0]
	}
	return chain(e.errors)
}

func (e *multiError) StackTrace() StackTrace {
	return e.trace
}

func (e *multiError) Append(errs ...error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	for _, err := range errs {
		if err == nil {
			panic("error cannot be nil")
		}

		// Append
		// nolint: errorlint
		switch v := err.(type) {
		case nestedErrorGetter:
			e.errors = append(e.errors, err)
		case multiErrorGetter:
			e.errors = append(e.errors, v.WrappedErrors()...)
		default:
			if _, ok := err.(stackTracer); ok {
				e.errors = append(e.errors, err)
			} else {
				e.errors = append(e.errors, &withStack{err: err, stack: callers()})
			}
		}
	}
}

func (e *multiError) AppendNested(err error) NestedError {
	nested := NewNestedError(err)
	e.Append(nested)
	return nested
}

func (e *multiError) AppendWithStack(err error, stack StackTrace) {
	if err == nil {
		panic("error cannot be nil")
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	e.errors = append(e.errors, &withStack{err: err, stack: stack})
}

func (e *multiError) AppendWithPrefix(err error, prefix string) {
	e.AppendWithStack(PrefixError(err, prefix), callers())
}

func (e *multiError) AppendWithPrefixf(err error, format string, a ...any) {
	e.AppendWithStack(PrefixErrorf(err, format, a...), callers())
}

// chain is a utility for errors.Is/As/Unwrap for MultiError.
// Unwrap returns next error in sequence.
type chain []error

func (e chain) Error() string {
	return e[0].Error()
}

func (e chain) Unwrap() error {
	if len(e) <= 1 {
		// No next error
		return nil
	}

	// Return next errors
	return e[1:]
}

func (e chain) As(target any) bool {
	if len(e) == 0 {
		return false
	}
	return As(e[0], target)
}

func (e chain) Is(target error) bool {
	if len(e) == 0 {
		return false
	}
	return Is(e[0], target)
}
