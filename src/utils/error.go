package utils

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type multiError = multierror.Error

type Error struct {
	*multiError
	lock *sync.Mutex
}

type ErrorRaw struct {
	msg string
}

func (e *ErrorRaw) Error() string {
	return e.msg
}

func NewMultiError() *Error {
	e := &Error{multiError: &multierror.Error{}, lock: &sync.Mutex{}}
	e.ErrorFormat = formatError
	return e
}

// Append error.
func (e *Error) Append(err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.multiError = multierror.Append(e.multiError, err)
}

// AppendRaw - msg will be printed without prefix.
func (e *Error) AppendRaw(msg string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.multiError = multierror.Append(e.multiError, &ErrorRaw{msg: msg})
}

// AppendWithPrefix - add an error with custom prefix.
func (e *Error) AppendWithPrefix(prefix string, err error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.multiError = multierror.Append(e.multiError, PrefixError(prefix, err))
}

func PrefixError(prefix string, err error) *Error {
	e := NewMultiError()
	e.Append(fmt.Errorf("%s:\n%s", prefix, prefixEachLine("\t- ", err.Error())))
	return e
}

// prefixEachLine 1. use prefix only once, 2. keep indentation, see tests.
func prefixEachLine(prefix, str string) string {
	return regexp.
		MustCompile(fmt.Sprintf(`((^|\n)(\s*)(%s)?\s*)`, regexp.QuoteMeta(strings.TrimSpace(prefix)))).
		ReplaceAllString(str, fmt.Sprintf("${2}${3}%s", regexp.QuoteMeta(prefix)))
}

// formatError formats the nested errors.
func formatError(errors []error) string {
	// Count errors without raw messages
	count := 0
	for _, err := range errors {
		if _, ok := err.(*ErrorRaw); !ok {
			count++
		}
	}

	// Prefix if there are more than 1 error
	var prefix string
	if count <= 1 {
		prefix = ``
	} else {
		prefix = `- `
	}

	// Prefix each error, format nested errors
	lines := make([]string, 0)
	for _, err := range errors {
		var errStr string
		switch v := err.(type) {
		case *ErrorRaw:
			errStr = v.Error()
		case *Error:
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
