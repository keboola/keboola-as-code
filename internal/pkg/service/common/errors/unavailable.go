package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ServiceUnavailableError struct {
	err         error
	userMessage string
}

func NewServiceUnavailableError(err error) ServiceUnavailableError {
	return ServiceUnavailableError{err: err}
}

func (ServiceUnavailableError) ErrorName() string {
	return "serviceUnavailable"
}

func (e ServiceUnavailableError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e ServiceUnavailableError) Unwrap() error {
	return e.err
}

func (e ServiceUnavailableError) Error() string {
	return e.err.Error()
}

func (e ServiceUnavailableError) WithUserMessage(msg string) ServiceUnavailableError {
	e.userMessage = msg
	return e
}

func (e ServiceUnavailableError) ErrorUserMessage() string {
	if e.userMessage != "" {
		return e.userMessage
	}
	return errors.Format(e, errors.FormatAsSentences())
}
