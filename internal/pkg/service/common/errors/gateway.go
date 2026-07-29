package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type BadGatewayError struct {
	err         error
	userMessage string
	logMessage  string
}

func NewBadGatewayError(err error) BadGatewayError {
	return BadGatewayError{err: err}
}

func (BadGatewayError) ErrorName() string {
	return "badGateway"
}

func (e BadGatewayError) StatusCode() int {
	return http.StatusBadGateway
}

func (e BadGatewayError) Unwrap() error {
	return e.err
}

func (e BadGatewayError) Error() string {
	return e.err.Error()
}

func (e BadGatewayError) WithUserMessage(msg string) BadGatewayError {
	e.userMessage = msg
	return e
}

// WithLogMessage sets the message written to the log, instead of the wrapped
// error. Use it when the wrapped error must not reach the user, so the wrapped
// error can be replaced by a safe one and its details kept for the log only.
func (e BadGatewayError) WithLogMessage(msg string) BadGatewayError {
	e.logMessage = msg
	return e
}

func (e BadGatewayError) ErrorLogMessage() string {
	if e.logMessage != "" {
		return e.logMessage
	}
	return errors.Format(e.err, errors.FormatWithUnwrap(), errors.FormatWithStack())
}

func (e BadGatewayError) ErrorUserMessage() string {
	if e.userMessage != "" {
		return e.userMessage
	}
	return errors.Format(e, errors.FormatAsSentences())
}
