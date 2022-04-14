package service

import (
	"net/http"
)

type NotImplementedError struct{}

func (NotImplementedError) ErrorName() string {
	return "NotImplemented"
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
	return "BadRequest"
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
