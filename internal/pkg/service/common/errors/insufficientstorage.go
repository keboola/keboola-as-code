package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type InsufficientStorageError struct {
	log bool
	err error
}

func NewInsufficientStorageError(log bool, err error) InsufficientStorageError {
	return InsufficientStorageError{log: log, err: err}
}

func (InsufficientStorageError) ErrorName() string {
	return "insufficientStorage"
}

func (e InsufficientStorageError) StatusCode() int {
	return http.StatusInsufficientStorage
}

func (e InsufficientStorageError) Error() string {
	return e.err.Error()
}

func (e InsufficientStorageError) ErrorUserMessage() string {
	return errors.Format(e, errors.FormatAsSentences())
}

func (e InsufficientStorageError) ErrorLogEnabled() bool {
	return e.log
}
