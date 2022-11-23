package errors

import (
	"net/http"
)

type EndpointNotFoundError struct{}

func NewEndpointNotFoundError() EndpointNotFoundError {
	return EndpointNotFoundError{}
}

func (EndpointNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (EndpointNotFoundError) Error() string {
	return "endpoint not found"
}

func (EndpointNotFoundError) ErrorName() string {
	return "endpointNotFound"
}

func (EndpointNotFoundError) ErrorUserMessage() string {
	return "Endpoint not found."
}
