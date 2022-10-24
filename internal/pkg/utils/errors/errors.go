// Package errors extends standard error handling with MultiError, configurable formatting and error stack trace.
//
// All errors created by the package contain stack trace.
//
// The stack trace can be included in the formatted output by [FormatWithStack] option.
//
// # Create Error
//
// [New] error from a string:
//
//	err := errors.New("some error")
//
// Use [Errorf] to wrap an existing error and include an original error message in the new error:
//
//	originalErr := errors.New("original error")
//	wrappedErr := errors.Errorf("enhanced error message: %w", originalErr)
//
// [Wrap] an existing error with different error message:
//
//	originalErr := errors.New("original error")
//	wrappedErr := errors.Wrap(originalErr, "new error message")
//
// Or use [Wrapf]:
//
//	originalErr := errors.New("original error")
//	wrappedErr := errors.Wrapf(originalErr, "new error %s", "message")
//
// Use [WithStack] to add stack trace to an existing error:
//
//	err := errors.WithStack(wrappedErr)
//
// # Format Error
//
// All errors created by the package use the default formatter if Error method is called:
//
//	fmt.Println(err.Error())
//
// For extended formatting options, use the [Format] function:
//
//	fmt.Println(Format(err))
//
// Use [FormatWithStack] option to include last stack frame in the output:
//
//	fmt.Println(errors.Format(err, errors.FormatWithStack()))
//
// Use [FormatWithUnwrap] option to see also all wrapped errors:
//
//	fmt.Println(errors.Format(err, errors.FormatWithUnwrap()))
//
// Use [FormatAsSentences] option to convert errors messages to sentences:
//
//	fmt.Println(errors.Format(err, errors.FormatAsSentences()))
//
// # Multi Error
//
// MultiError allows errors to be composed into a nested structure.
//
//	errs := errors.NewMultiError()
//	errs.Append(errors.New("foo 1"))
//	errs.Append(errors.New("foo 2"))
//
//	sub := errs.AppendNested(errors.New("some sub error"))
//	sub.Append(errors.New("foo 3"))
//	sub.Append(errors.New("foo 4"))
//
//	errs.AppendWithPrefixf(errors.New("nested error"), "some %s", "prefix")
//
//	return errs.ErrorOrNil()
//
// # Custom Error Formatting
//
// If an error implements WriteError method, then it is used to format the error:
//
//	WriteError(w Writer, level int, trace StackTrace)
//
// Example - a custom multi-line error formatting:
//
//	 func (e UnencryptedValueError) WriteError(w errors.Writer, level int, trace errors.StackTrace) {
//	   w.WritePrefix(e.Error(), trace)
//	   w.WriteNewLine()
//
//	   last := len(v.values) - 1
//	   for i, value := range e.values {
//	     w.WriteIndent(level)
//	     w.WriteBullet()
//	     w.Write(value)
//	     if i != last {
//	       w.WriteNewLine()
//	     }
//	   }
//	}
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

type wrappedError struct {
	message  string
	trace    StackTrace
	original error
}

func (e *wrappedError) Error() string {
	return e.message
}

func (e *wrappedError) StackTrace() StackTrace {
	return e.trace
}

func (e *wrappedError) Unwrap() error {
	return e.original
}

// Wrap wraps the error with a different message.
func Wrap(err error, msg string) error {
	return &wrappedError{message: msg, trace: callers(), original: err}
}

// Wrapf wraps the error with a different message.
// It is similar to Errorf, but the original error is not part of the message at all.
func Wrapf(err error, format string, a ...any) error {
	return &wrappedError{message: fmt.Sprintf(format, a...), trace: callers(), original: err}
}
