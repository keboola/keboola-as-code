package service

import (
	"net/http"
)

type NotImplementedError struct{}

func (NotImplementedError) ErrorName() string {
	return "notImplemented"
}

func (NotImplementedError) Error() string {
	return "operation not implemented"
}

func (NotImplementedError) ErrorUserMessage() string {
	return "Operation not implemented."
}

type BadRequestError struct {
	Message string
}

func (BadRequestError) ErrorName() string {
	return "badRequest"
}

func (e BadRequestError) StatusCode() int {
	return http.StatusBadRequest
}

func (e BadRequestError) Error() string {
	return e.Message
}

func (e BadRequestError) ErrorUserMessage() string {
	return e.Message
}
