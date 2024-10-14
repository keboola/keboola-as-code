package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type UnprocessableContentError struct {
	err         error
	userMessage string
}

func NewUnprocessableContentError(err error) UnprocessableContentError {
	return UnprocessableContentError{err: err}
}

func (UnprocessableContentError) ErrorName() string {
	return "unprocessableContent"
}

func (e UnprocessableContentError) StatusCode() int {
	return http.StatusUnprocessableEntity
}

func (e UnprocessableContentError) Unwrap() error {
	return e.err
}

func (e UnprocessableContentError) Error() string {
	return e.err.Error()
}

func (e UnprocessableContentError) WithUserMessage(msg string) UnprocessableContentError {
	e.userMessage = msg
	return e
}

func (e UnprocessableContentError) ErrorUserMessage() string {
	if e.userMessage != "" {
		return e.userMessage
	}
	return errors.Format(e, errors.FormatAsSentences())
}
