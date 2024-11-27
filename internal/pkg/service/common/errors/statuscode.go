package errors

import (
	"context"
	"net/http"

	goa "goa.design/goa/v3/pkg"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	StatusClientClosedRequest = 499
)

type withStatusCodeError struct {
	error
	httpCode int
}

func HTTPCodeFrom(err error) int {
	var httpCodeProvider WithStatusCode
	if errors.As(err, &httpCodeProvider) {
		return httpCodeProvider.StatusCode()
	}

	var serviceError *goa.ServiceError
	if errors.As(err, &serviceError) {
		return http.StatusBadRequest
	}

	// Handle client closed request
	if errors.Is(err, context.Canceled) {
		// https://httpstatus.in/499/
		// A non-standard status code introduced by nginx for the case
		// when a client closes the connection while server is processing the request.
		// The code is used for telemetry and logging purposes, since the connection is closed.
		return StatusClientClosedRequest
	}

	// Handle request timeout
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}

	return http.StatusInternalServerError
}

func WrapWithStatusCode(err error, httpCode int) WithStatusCode {
	return withStatusCodeError{error: err, httpCode: httpCode}
}

func (w withStatusCodeError) StatusCode() int {
	return w.httpCode
}

func (w withStatusCodeError) Unwrap() error {
	return w.error
}
