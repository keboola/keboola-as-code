package httpserver

import (
	"context"
	"net/http"
	"strings"

	"github.com/iancoleman/strcase"
	"go.opentelemetry.io/otel/attribute"
	goaHTTP "goa.design/goa/v3/http"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
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
	// Get or generate requestID
	requestID, ok := ctx.Value(middleware.RequestIDCtxKey).(string)
	if !ok {
		requestID = idgenerator.RequestID()
	}

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
	var logEnabledProvider WithErrorLogEnabled
	if !errors.As(err, &logEnabledProvider) || logEnabledProvider.ErrorLogEnabled() {
		// Log message
		var logMessage string
		var logMessageProvider WithLogMessage
		if errors.As(err, &logMessageProvider) {
			logMessage = logMessageProvider.ErrorLogMessage()
		} else {
			logMessage = errors.Format(err, errors.FormatWithStack())
		}

		// Attributes
		attrs := datadog.ErrorAttrs(err)
		if response.ExceptionID != nil {
			attrs = append(attrs, attribute.String("exceptionId", *response.ExceptionID))
		}
		if response.Name != "" {
			attrs = append(attrs, attribute.String("error.name", response.Name))
		}
		attrs = append(attrs, attribute.String("response", json.MustEncodeString(response, false)))

		// Level
		logger := wr.logger.With(attrs...)
		if response.StatusCode > 499 {
			logger.Error(ctx, logMessage)
		} else {
			logger.Info(ctx, logMessage)
		}
	}

	// Write response
	_, err = w.Write([]byte(json.MustEncodeString(response, true)))
	return err
}
