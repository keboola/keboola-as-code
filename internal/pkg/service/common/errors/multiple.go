package errors

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type multipleErrors struct {
	error
	httpCode int
}

func WrapMultipleErrors(err error, httpCode int) WithStatusCode {
	err = errors.PrefixError(err, "multiple errors occurred")
	return multipleErrors{error: err, httpCode: httpCode}
}

func (w multipleErrors) Unwrap() error {
	return w.error
}

func (w multipleErrors) StatusCode() int {
	return w.httpCode
}

func (w multipleErrors) ErrorName() string {
	return "multipleErrors"
}

func (w multipleErrors) ErrorUserMessage() string {
	return w.error.Error()
}
