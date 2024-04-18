package errors

import (
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ResourceNotFoundError struct {
	what string
	key  string
	in   string
	err  error
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
	return errors.Format(e, errors.FormatAsSentences())
}

func (e ResourceNotFoundError) Wrap(err error) ResourceNotFoundError {
	e.err = err
	return e
}

func (e ResourceNotFoundError) Unwrap() error {
	return e.err
}
