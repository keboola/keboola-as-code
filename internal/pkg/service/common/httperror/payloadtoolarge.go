package httperror

import (
	"net/http"
)

type PayloadTooLargeError struct {
	Message string
}

func (PayloadTooLargeError) ErrorName() string {
	return "payloadTooLarge"
}

func (e PayloadTooLargeError) StatusCode() int {
	return http.StatusRequestEntityTooLarge
}

func (e PayloadTooLargeError) Error() string {
	return e.Message
}

func (e PayloadTooLargeError) ErrorUserMessage() string {
	return e.Message
}
