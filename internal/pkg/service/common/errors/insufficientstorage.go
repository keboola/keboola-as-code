package errors

import (
	"net/http"
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
	return e.Error()
}

func (e InsufficientStorageError) ErrorLogEnabled() bool {
	return e.log
}
