package errors

import (
	"fmt"
	"net/http"
	"runtime/debug"
)

func NewPanicError(value any) PanicError {
	return PanicError{value: value}
}

type PanicError struct {
	value any
}

func (PanicError) StatusCode() int {
	return http.StatusInternalServerError
}

func (e PanicError) Error() string {
	return fmt.Sprintf("%s", e.value)
}

func (e PanicError) ErrorLogMessage() string {
	return fmt.Sprintf("panic=%s stacktrace=%s", e.value, string(debug.Stack()))
}

func (PanicError) ErrorName() string {
	return "panicError"
}
