package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type BadGatewayError struct {
	err         error
	userMessage string
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

func (e BadGatewayError) ErrorUserMessage() string {
	if e.userMessage != "" {
		return e.userMessage
	}
	return errors.Format(e, errors.FormatAsSentences())
}
