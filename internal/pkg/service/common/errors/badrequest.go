package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type BadRequestError struct {
	err error
}

func NewBadRequestError(err error) BadRequestError {
	return BadRequestError{err: err}
}

func (BadRequestError) ErrorName() string {
	return "badRequest"
}

func (e BadRequestError) StatusCode() int {
	return http.StatusBadRequest
}

func (e BadRequestError) Unwrap() error {
	return e.err
}

func (e BadRequestError) Error() string {
	return e.err.Error()
}

func (e BadRequestError) ErrorUserMessage() string {
	return errors.Format(e.err, errors.FormatAsSentences())
}
