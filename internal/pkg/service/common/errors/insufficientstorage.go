package errors

import (
	"net/http"
)

type InsufficientStorageError struct {
	err error
}

func NewInsufficientStorageError(err error) InsufficientStorageError {
	return InsufficientStorageError{err: err}
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
