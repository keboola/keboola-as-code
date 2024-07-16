package router

import (
	"net/http"
)

type ShutdownError struct{}

func (e ShutdownError) Error() string {
	return "node is shutting down"
}

func (ShutdownError) ErrorName() string {
	return "shutdown"
}

func (ShutdownError) StatusCode() int {
	return http.StatusServiceUnavailable
}
