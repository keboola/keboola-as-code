package errors

import (
	"net/http"
)

type PayloadTooLargeError struct {
	err error
}

func NewPayloadTooLargeError(err error) PayloadTooLargeError {
	return PayloadTooLargeError{err: err}
}

func (PayloadTooLargeError) ErrorName() string {
	return "payloadTooLarge"
}

func (e PayloadTooLargeError) StatusCode() int {
	return http.StatusRequestEntityTooLarge
}

func (e PayloadTooLargeError) Error() string {
	return e.err.Error()
}

func (e PayloadTooLargeError) ErrorUserMessage() string {
	return e.Error()
}
