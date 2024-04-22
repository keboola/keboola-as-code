package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ForbiddenError struct {
	err error
}

func NewForbiddenError(err error) ForbiddenError {
	return ForbiddenError{err: err}
}

func (ForbiddenError) ErrorName() string {
	return "forbidden"
}

func (e ForbiddenError) StatusCode() int {
	return http.StatusForbidden
}

func (e ForbiddenError) Unwrap() error {
	return e.err
}

func (e ForbiddenError) Error() string {
	return e.err.Error()
}

func (e ForbiddenError) ErrorUserMessage() string {
	return errors.Format(e, errors.FormatAsSentences())
}
