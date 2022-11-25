package errors

import (
	"fmt"
	"net/http"
)

type ResourceNotFoundError struct {
	what string
	key  string
}

func NewResourceNotFoundError(what, key string) ResourceNotFoundError {
	return ResourceNotFoundError{what: what, key: key}
}

func (e ResourceNotFoundError) ErrorName() string {
	return fmt.Sprintf("%sNotFound", e.what)
}

func (e ResourceNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (e ResourceNotFoundError) Error() string {
	return fmt.Sprintf(`%s "%s" not found`, e.what, e.key)
}

func (e ResourceNotFoundError) ErrorUserMessage() string {
	return e.Error()
}
