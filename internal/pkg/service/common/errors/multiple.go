package errors

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type multipleErrors struct { // nolint:errname
	error
	httpCode int
}

func WrapMultipleErrors(err error, httpCode int) WithStatusCode {
	err = errors.PrefixError(err, "multiple errors occurred")
	return multipleErrors{error: err, httpCode: httpCode}
}

func (e multipleErrors) Unwrap() error {
	return e.error
}

func (e multipleErrors) StatusCode() int {
	return e.httpCode
}

func (e multipleErrors) ErrorName() string {
	return "multipleErrors"
}

func (e multipleErrors) ErrorUserMessage() string {
	return errors.Format(e, errors.FormatAsSentences())
}
