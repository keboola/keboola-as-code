package errors

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type EOFError struct {
	err error
}

func NewIOError(err error) EOFError {
	return EOFError{err: err}
}

func (EOFError) ErrorName() string {
	return "eof"
}

func (e EOFError) StatusCode() int {
	return 499 // unofficial status code for "client closed the connection"
}

func (e EOFError) Unwrap() error {
	return e.err
}

func (e EOFError) Error() string {
	return e.err.Error()
}

func (e EOFError) ErrorUserMessage() string {
	return errors.Format(e.err, errors.FormatAsSentences())
}
