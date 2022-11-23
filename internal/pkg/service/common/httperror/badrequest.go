// Package httperror provides basic errors for all APIs.
package httperror

import (
	"net/http"
)

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
