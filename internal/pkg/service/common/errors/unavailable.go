package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ServiceUnavailableError struct {
	err         error
	userMessage string
	logMessage  string
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

// WithLogMessage sets the detail written to the log, instead of the wrapped
// error. Use it when the wrapped error must not reach the user.
// Pass the raw detail; ErrorLogMessage adds the error name prefix.
func (e ServiceUnavailableError) WithLogMessage(msg string) ServiceUnavailableError {
	e.logMessage = msg
	return e
}

func (e ServiceUnavailableError) ErrorLogMessage() string {
	detail := e.logMessage
	if detail == "" {
		detail = errors.Format(e.err, errors.FormatWithUnwrap(), errors.FormatWithStack())
	}
	return e.ErrorName() + ": " + detail
}

func (e ServiceUnavailableError) ErrorUserMessage() string {
	if e.userMessage != "" {
		return e.userMessage
	}
	return errors.Format(e, errors.FormatAsSentences())
}
