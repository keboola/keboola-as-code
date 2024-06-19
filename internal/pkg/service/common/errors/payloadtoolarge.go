package errors

import (
	"net/http"
)

type PayloadTooLargeError struct {
	err error
}

type HeaderTooLargeError struct {
	err error
}

type BodyTooLargeError struct {
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

func NewHeaderTooLargeError(err error) HeaderTooLargeError {
	return HeaderTooLargeError{err: err}
}

func (HeaderTooLargeError) ErrorName() string {
	return "headerTooLarge"
}

func (e HeaderTooLargeError) StatusCode() int {
	return http.StatusRequestEntityTooLarge
}

func (e HeaderTooLargeError) Error() string {
	return e.err.Error()
}

func (e HeaderTooLargeError) ErrorUserMessage() string {
	return e.Error()
}

func NewBodyTooLargeError(err error) BodyTooLargeError {
	return BodyTooLargeError{err: err}
}

func (BodyTooLargeError) ErrorName() string {
	return "bodyTooLarge"
}

func (e BodyTooLargeError) StatusCode() int {
	return http.StatusRequestEntityTooLarge
}

func (e BodyTooLargeError) Error() string {
	return e.err.Error()
}

func (e BodyTooLargeError) ErrorUserMessage() string {
	return e.Error()
}
