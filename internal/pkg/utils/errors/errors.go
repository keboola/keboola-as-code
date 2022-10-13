// Package errors extends standard error handling with MultiError and error's stack trace.
package errors

import (
	"errors"
	"fmt"
)

// Re-export standard functions

// New returns an error from the given text, stack trace is included.
func New(text string) error {
	return WithStack(errors.New(text))
}

// Is reports whether any error in chain matches target.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As finds the first error in chain that matches target, and if one is found, sets.
func As(err error, target any) bool {
	return errors.As(err, target)
}

// Unwrap returns the result of calling the Unwrap method on err.
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// Errorf formats according to a format specifier and returns the string as a
// value that satisfies error.
func Errorf(format string, a ...any) error {
	return WithStack(fmt.Errorf(format, a...))
}
