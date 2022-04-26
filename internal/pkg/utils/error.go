package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/umisama/go-regexpcache"
)

type multiError = multierror.Error

type MultiError struct {
	*multiError
	lock *sync.Mutex
}

type NestedError struct {
	*MultiError
	Msg string
}

func NewMultiError() *MultiError {
	e := &MultiError{multiError: &multierror.Error{}, lock: &sync.Mutex{}}
	e.ErrorFormat = NewErrorFormatter().multiErrFormatter
	return e
}

func PrefixError(msg string, err error) *NestedError {
	nested := NewMultiError()
	if !errors.As(err, &nested) {
		nested.Append(err)
	}

	return &NestedError{
		MultiError: nested,
		Msg:        msg,
	}
}

// Append error.
func (e *MultiError) Append(err error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Unwrap multi error, so it can be flattened
	if v, ok := err.(*MultiError); ok { // nolint: errorlint
		err = v.multiError
	}
	e.multiError = multierror.Append(e.multiError, err)
}

// AppendWithPrefix - add an error with custom prefix.
func (e *MultiError) AppendWithPrefix(prefix string, err error) {
	e.Append(PrefixError(prefix, err))
}

func (e *NestedError) Error() (out string) {
	return e.ErrorFormat([]error{e})
}

type MsgFormatFunc func(string) string

type ErrorFormatter struct {
	multiErrFormatter multierror.ErrorFormatFunc
	errorMsgFormatter MsgFormatFunc
}

func NewErrorFormatter() *ErrorFormatter {
	f := &ErrorFormatter{}
	f.multiErrFormatter = multiErrFormatter(f)               // default
	f.errorMsgFormatter = func(s string) string { return s } // default nop
	return f
}

func (f *ErrorFormatter) MultiErrorFormatter(fn multierror.ErrorFormatFunc) {
	f.multiErrFormatter = fn
}

func (f *ErrorFormatter) ErrorMessageFormatter(fn MsgFormatFunc) {
	f.errorMsgFormatter = fn
}

// Format formats nested errors.
func (f ErrorFormatter) Format(err error) string {
	// nolint: errorlint
	switch v := err.(type) {
	case *MultiError:
		return f.multiErrFormatter(v.Errors)
	case *multierror.Error:
		return f.multiErrFormatter(v.Errors)
	case *NestedError:
		l := v.MultiError.Len()
		if l == 0 {
			return f.errorMsgFormatter(v.Msg)
		}

		// If there is > 1 error or the msg is long,
		// then break line and create bullet list
		errStr := f.errorMsgFormatter(v.Msg+":") + " " + f.multiErrFormatter(v.Errors)
		firstLine := strings.SplitN(errStr, "\n", 2)[0]
		if l > 1 || len(firstLine) > 60 {
			// Break line and force bullet list
			errStr = f.errorMsgFormatter(v.Msg+":") + "\n" + prefixEachLine("  - ", f.multiErrFormatter(v.Errors))
		}
		return errStr
	default:
		return f.errorMsgFormatter(v.Error())
	}
}

func multiErrFormatter(f *ErrorFormatter) multierror.ErrorFormatFunc {
	return func(errors []error) string {
		// Create bullet list if there are more than 1 error
		var prefix string
		if len(errors) <= 1 {
			prefix = ``
		} else {
			prefix = `- `
		}

		// Format nested errors
		var out strings.Builder
		for _, err := range errors {
			msg := strings.TrimRight(f.Format(err), "\n")
			out.WriteString(prefixEachLine(prefix, msg))
			out.WriteString("\n")
		}
		return strings.TrimRight(out.String(), "\n")
	}
}

// prefixEachLine 1. use prefix only once, 2. keep indentation, see tests.
func prefixEachLine(prefix, str string) string {
	return regexpcache.
		MustCompile(fmt.Sprintf(`((^|\n)(\s*)(%s)?\s*)`, regexp.QuoteMeta(strings.TrimSpace(prefix)))).
		ReplaceAllString(str, fmt.Sprintf("${2}${3}%s", regexp.QuoteMeta(prefix)))
}
