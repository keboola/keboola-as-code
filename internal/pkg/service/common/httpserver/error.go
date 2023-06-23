package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/iancoleman/strcase"
	goaHTTP "goa.design/goa/v3/http"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DefaultErrorName    = "internalError"
	DefaultErrorMessage = "Application error. Please contact our support support@keboola.com with exception id (%s) attached."
)

type ErrorHandler func(context.Context, http.ResponseWriter, error)

type ErrorFormatter func(ctx context.Context, err error) goaHTTP.Statuser

type ErrorWriter struct {
	logger            log.Logger
	errorNamePrefix   string
	exceptionIDPrefix string
}

func NewErrorWriter(logger log.Logger, errorNamePrefix, exceptionIDPrefix string) ErrorWriter {
	return ErrorWriter{logger: logger, errorNamePrefix: errorNamePrefix, exceptionIDPrefix: exceptionIDPrefix}
}

func (wr *ErrorWriter) WriteWithStatusCode(ctx context.Context, w http.ResponseWriter, err error) {
	w.WriteHeader(HTTPCodeFrom(err))
	_ = wr.WriteOrErr(ctx, w, err)
}

func (wr *ErrorWriter) Write(ctx context.Context, w http.ResponseWriter, err error) {
	_ = wr.WriteOrErr(ctx, w, err)
}

func (wr *ErrorWriter) WriteOrErr(ctx context.Context, w http.ResponseWriter, err error) error {
	requestID, _ := ctx.Value(middleware.RequestIDCtxKey).(string)

	// Default values
	response := &UnexpectedError{
		StatusCode:  HTTPCodeFrom(err),
		Name:        DefaultErrorName,
		Message:     err.Error(),
		ExceptionID: nil,
	}

	// Error name
	var nameProvider WithName
	if errors.As(err, &nameProvider) {
		response.Name = nameProvider.ErrorName()
	}

	// Normalize error name
	if !strings.Contains(response.Name, ".") {
		// Normalize error name, e.g., "missing_field" to "buffer.missingField"
		response.Name = wr.errorNamePrefix + strcase.ToLowerCamel(response.Name)
	}

	// Re-use exception ID from Storage or other API, if possible.
	// Otherwise, generate custom exception ID.
	var exceptionIDProvider WithExceptionID
	if errors.As(err, &exceptionIDProvider) {
		v := exceptionIDProvider.ErrorExceptionId()
		response.ExceptionID = &v
	} else if response.StatusCode > 499 {
		v := wr.exceptionIDPrefix + requestID
		response.ExceptionID = &v
	}

	// Error message
	var errForResponse error
	var messageProvider WithUserMessage
	switch {
	case errors.As(err, &messageProvider):
		errForResponse = errors.New(messageProvider.ErrorUserMessage())
	case response.StatusCode > 499:
		errForResponse = errors.Errorf(DefaultErrorMessage, *response.ExceptionID)
	default:
		errForResponse = err
	}

	// Convert error to response message
	response.Message = errors.Format(errForResponse, errors.FormatAsSentences())

	// Log error
	logger := wr.logger.AddPrefix(fmt.Sprintf("[http][requestId=%s]", requestID))
	if response.StatusCode > 499 {
		logger.Error(errorLogMessage(err, response))
	} else {
		logger.Info(errorLogMessage(err, response))
	}

	// Write response
	_, err = w.Write([]byte(json.MustEncodeString(response, true)))
	return err
}

func errorLogMessage(err error, response *UnexpectedError) string {
	// Log exception ID if it is present
	exceptionIDValue := ""
	if response.ExceptionID != nil {
		exceptionIDValue = "exceptionId=" + *response.ExceptionID + " "
	}

	// Custom log message
	var errWithLogMessage WithLogMessage
	if errors.As(err, &errWithLogMessage) {
		return exceptionIDValue + errWithLogMessage.ErrorLogMessage()
	}

	// Format message
	return fmt.Sprintf(
		"%s | %serrorName=%s errorType=%T response=%s",
		errors.Format(err, errors.FormatWithStack()), exceptionIDValue, response.Name, err, json.MustEncodeString(response, false),
	)
}
