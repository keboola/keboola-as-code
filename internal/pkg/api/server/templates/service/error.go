package service

import (
	"net/http"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type NotImplementedError struct{}

func (NotImplementedError) ErrorName() string {
	return "notImplemented"
}

func (NotImplementedError) Error() string {
	return "operation not implemented"
}

func (NotImplementedError) ErrorUserMessage() string {
	return "Operation not implemented."
}

type BadRequestError struct {
	Message string
}

func (BadRequestError) ErrorName() string {
	return "badRequest"
}

func (e BadRequestError) StatusCode() int {
	return http.StatusBadRequest
}

func (e BadRequestError) Error() string {
	return e.Message
}

func (e BadRequestError) ErrorUserMessage() string {
	return e.Message
}

func NewValidationErrorFormatter() errors.Formatter {
	return errors.
		NewFormatter().
		WithMessageFormatter(func(s string, _ errors.StackTrace) string {
			// Uppercase first letter
			s = strhelper.FirstUpper(s)

			// Add period if the message ends with an alphanumeric character
			lastChar := s[len(s)-1:]
			if regexpcache.MustCompile("^[a-zA-Z0-9]$").MatchString(lastChar) {
				s += "."
			}
			return s
		})
}
