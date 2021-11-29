package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type multiError = multierror.Error

type MultiError struct {
	*multiError
	lock *sync.Mutex
}

type NestedError struct {
	*MultiError
	msg string
}

func (e *NestedError) Error() (out string) {
	l := e.MultiError.Len()

	switch {
	case l == 0:
		out = e.msg
	case l == 1:
		out = e.msg + `: ` + e.MultiError.Error()
	}

	// If there is > 1 error or the msg is long,
	// then break line and create bullet list
	firstLine := strings.SplitN(out, "\n", 2)[0]
	if l > 1 || len(firstLine) > 60 {
		out = e.msg + ":\n" + prefixEachLine("  - ", e.MultiError.Error())
	}

	return out
}

func NewMultiError() *MultiError {
	e := &MultiError{multiError: &multierror.Error{}, lock: &sync.Mutex{}}
	e.ErrorFormat = formatError
	return e
}

// Append error.
func (e *MultiError) Append(err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.multiError = multierror.Append(e.multiError, err)
}

// AppendWithPrefix - add an error with custom prefix.
func (e *MultiError) AppendWithPrefix(prefix string, err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.multiError = multierror.Append(e.multiError, PrefixError(prefix, err))
}

func PrefixError(msg string, err error) *NestedError {
	nested := NewMultiError()
	if !errors.As(err, &nested) {
		nested.Append(err)
	}

	return &NestedError{
		MultiError: nested,
		msg:        msg,
	}
}

// prefixEachLine 1. use prefix only once, 2. keep indentation, see tests.
func prefixEachLine(prefix, str string) string {
	return regexp.
		MustCompile(fmt.Sprintf(`((^|\n)(\s*)(%s)?\s*)`, regexp.QuoteMeta(strings.TrimSpace(prefix)))).
		ReplaceAllString(str, fmt.Sprintf("${2}${3}%s", regexp.QuoteMeta(prefix)))
}

// formatError formats the nested errors.
func formatError(errors []error) string {
	// Prefix if there are more than 1 error
	var prefix string
	if len(errors) <= 1 {
		prefix = ``
	} else {
		prefix = `- `
	}

	// Prefix each error, format nested errors
	lines := make([]string, 0)
	for _, err := range errors {
		var errStr string
		// nolint: errorlint
		switch v := err.(type) {
		case *MultiError:
			errStr = prefixEachLine(prefix, formatError(v.Errors))
		case *multierror.Error:
			errStr = prefixEachLine(prefix, formatError(v.Errors))
		default:
			errStr = prefixEachLine(prefix, v.Error())
		}

		lines = append(lines, errStr)
	}

	return strings.Join(lines, "\n")
}
