package errors

import (
	"fmt"
	"net/http"
)

type ResourceNotFoundError struct {
	what string
	key  string
	in   string
}

func NewResourceNotFoundError(what, key, in string) ResourceNotFoundError {
	return ResourceNotFoundError{what: what, key: key, in: in}
}

func NewNoResourceFoundError(what, in string) ResourceNotFoundError {
	return ResourceNotFoundError{what: what, in: in}
}

func (e ResourceNotFoundError) ErrorName() string {
	return fmt.Sprintf("%sNotFound", e.what)
}

func (e ResourceNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (e ResourceNotFoundError) Error() string {
	if e.key == "" {
		return fmt.Sprintf(`no %s found in the %s`, e.what, e.in)
	}
	return fmt.Sprintf(`%s "%s" not found in the %s`, e.what, e.key, e.in)
}

func (e ResourceNotFoundError) ErrorUserMessage() string {
	return e.Error()
}
