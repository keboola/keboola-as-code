package errors

import (
	"net/http"

	goa "goa.design/goa/v3/pkg"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type withStatusCode struct {
	error
	httpCode int
}

func HTTPCodeFrom(err error) int {
	httpCode := http.StatusInternalServerError
	var httpCodeProvider WithStatusCode
	var serviceError *goa.ServiceError
	if errors.As(err, &httpCodeProvider) {
		httpCode = httpCodeProvider.StatusCode()
	} else if errors.As(err, &serviceError) {
		httpCode = http.StatusBadRequest
	}
	return httpCode
}

func WrapWithStatusCode(err error, httpCode int) WithStatusCode {
	return withStatusCode{error: err, httpCode: httpCode}
}

func (w withStatusCode) StatusCode() int {
	return w.httpCode
}

func (w withStatusCode) Unwrap() error {
	return w.error
}
