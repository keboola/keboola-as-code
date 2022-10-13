// nolint: errorlint
package http

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/iancoleman/strcase"
	goaHTTP "goa.design/goa/v3/http"
	"goa.design/goa/v3/middleware"
	goa "goa.design/goa/v3/pkg"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	ExceptionIdPrefix   = "keboola-templates-"
	ErrorNamePrefix     = "templates."
	DefaultErrorName    = "internalError"
	DefaultErrorMessage = "Application error. Please contact our support support@keboola.com with exception id (%s) attached."
)

type NotFoundError struct{}

type UnexpectedError struct {
	// HTTP status code.
	StatusCode int `json:"statusCode"`
	// Name of error.
	Name string `json:"error"`
	// Error message.
	Message string `json:"message"`
	// ID of the error
	ExceptionID *string `json:"exceptionId,omitempty"`
}

type PanicError struct {
	Value interface{}
}

func (NotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (NotFoundError) Error() string {
	return "endpoint not found"
}

func (NotFoundError) ErrorName() string {
	return "endpointNotFound"
}

func (NotFoundError) ErrorUserMessage() string {
	return "Endpoint not found."
}

func (e UnexpectedError) Error() string {
	return e.Message
}

func (e UnexpectedError) ErrorName() string {
	return e.Name
}

func (PanicError) StatusCode() int {
	return http.StatusInternalServerError
}

func (e PanicError) Error() string {
	return fmt.Sprintf("%s", e.Value)
}

func (e PanicError) ErrorLogMessage() string {
	return fmt.Sprintf("panic=%s stacktrace=%s", e.Value, string(debug.Stack()))
}

func (PanicError) ErrorName() string {
	return "panicError"
}

type errorWithName interface {
	ErrorName() string
}

type errorWithUserMessage interface {
	ErrorUserMessage() string
}

type errorWithExceptionId interface {
	ErrorExceptionId() string
}

type errorWithLogMessage interface {
	ErrorLogMessage() string
}

type errorWithStatusCode struct {
	error
	httpCode int
}

func (w errorWithStatusCode) StatusCode() int {
	return w.httpCode
}

func (w errorWithStatusCode) Unwrap() error {
	return w.error
}

// errorHandler returns a function that writes and logs the given error.
func errorHandler() func(context.Context, http.ResponseWriter, error) {
	return handleError
}

// errorFormatter returns a function that adds HTTP status code to the given error.
func errorFormatter() func(err error) goaHTTP.Statuser {
	return formatError
}

// handleError handles an unexpected error.
func handleError(_ context.Context, w http.ResponseWriter, err error) {
	formattedErr := formatError(err)
	w.WriteHeader(formattedErr.StatusCode())
	_, _ = w.Write([]byte(json.MustEncodeString(formattedErr, true)))
}

// formatError sets HTTP status code to error.
func formatError(err error) goaHTTP.Statuser {
	return errorWithStatusCode{
		error:    err,
		httpCode: errorHttpCode(err),
	}
}

// writeError to HTTP response.
func writeError(ctx context.Context, logger log.Logger, w http.ResponseWriter, err error) error {
	// Default values
	response := &UnexpectedError{
		StatusCode:  http.StatusInternalServerError,
		Name:        DefaultErrorName,
		Message:     err.Error(),
		ExceptionID: nil,
	}

	// HTTP status code
	if v, ok := err.(errorWithStatusCode); ok {
		response.StatusCode = v.StatusCode()
		err = v.Unwrap()
	} else if v, ok := err.(goaHTTP.Statuser); ok {
		response.StatusCode = v.StatusCode()
	}

	// Error name
	var nameProvider errorWithName
	if errors.As(err, &nameProvider) {
		response.Name = nameProvider.ErrorName()
	}

	// Normalize error name
	if !strings.Contains(response.Name, ".") {
		// Normalize error name, eg., "missing_field" to "templates.missingField"
		response.Name = ErrorNamePrefix + strcase.ToLowerCamel(response.Name)
	}

	// Re-use exception ID from Storage or other API, if possible.
	// Otherwise, generate custom exception ID.
	var exceptionIdProvider errorWithExceptionId
	if errors.As(err, &exceptionIdProvider) {
		v := exceptionIdProvider.ErrorExceptionId()
		response.ExceptionID = &v
	} else {
		v := ExceptionIdPrefix + ctx.Value(middleware.RequestIDKey).(string)
		response.ExceptionID = &v
	}

	// Error message
	var messageProvider errorWithUserMessage
	switch {
	case errors.As(err, &messageProvider):
		response.Message = messageProvider.ErrorUserMessage()
	case response.StatusCode > 499:
		response.Message = fmt.Sprintf(DefaultErrorMessage, *response.ExceptionID)
	default:
		response.Message = err.Error()
	}

	// Normalize error message
	response.Message = strings.TrimSuffix(response.Message, ".") + "."
	response.Message = strhelper.FirstUpper(response.Message)

	// Log error
	if response.StatusCode > 499 {
		logger.Error(errorLogMessage(err, response))
	} else {
		logger.Info(errorLogMessage(err, response))
	}

	// Write response
	w.WriteHeader(response.StatusCode)
	_, wErr := w.Write([]byte(json.MustEncodeString(response, true)))
	return wErr
}

func errorLogMessage(err error, response *UnexpectedError) string {
	// Log exception ID if it is present
	exceptionIdValue := ""
	if response.ExceptionID != nil {
		exceptionIdValue = "exceptionId=" + *response.ExceptionID + " "
	}

	// Custom log message
	if v, ok := err.(errorWithLogMessage); ok {
		return exceptionIdValue + v.ErrorLogMessage()
	}

	// Format message
	return fmt.Sprintf(
		"%s | %serrorName=%s errorType=%T response=%s",
		err.Error(), exceptionIdValue, response.Name, err, json.MustEncodeString(response, false),
	)
}

func errorHttpCode(err error) int {
	httpCode := 500
	var httpCodeProvider goaHTTP.Statuser
	var serviceError *goa.ServiceError
	if errors.As(err, &httpCodeProvider) {
		httpCode = httpCodeProvider.StatusCode()
	} else if errors.As(err, &serviceError) {
		httpCode = http.StatusBadRequest
	}
	return httpCode
}
