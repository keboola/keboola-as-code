package errors

import (
	"net/http"
)

type UnsupportedMediaTypeError struct {
	err error
}

func NewUnsupportedMediaTypeError(err error) UnsupportedMediaTypeError {
	return UnsupportedMediaTypeError{err: err}
}

func (UnsupportedMediaTypeError) ErrorName() string {
	return "unsupportedMediaType"
}

func (e UnsupportedMediaTypeError) StatusCode() int {
	return http.StatusUnsupportedMediaType
}

func (e UnsupportedMediaTypeError) Error() string {
	return e.err.Error()
}

func (e UnsupportedMediaTypeError) ErrorUserMessage() string {
	return e.Error()
}
