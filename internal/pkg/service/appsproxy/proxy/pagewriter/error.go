package pagewriter

import (
	"net"
	"net/http"
	"strings"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type errorPageData struct {
	Status      int
	StatusText  string
	Messages    []string
	Details     string
	ExceptionID string
}

func (pw *Writer) ProxyErrorHandler(w http.ResponseWriter, req *http.Request, err error) {
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		pw.logger.Info(req.Context(), "app is not running, rendering spinner page")
		pw.WriteSpinnerPage(w, req)
	} else {
		err = svcerrors.NewBadGatewayError(err).WithUserMessage("Request to application failed.")
		pw.WriteError(w, req, err)
	}
}

func (pw *Writer) WriteError(w http.ResponseWriter, req *http.Request, err error) {
	// Status code, by default 500
	status := http.StatusInternalServerError
	var statusProvider svcerrors.WithStatusCode
	if errors.As(err, &statusProvider) {
		status = statusProvider.StatusCode()
	}

	// Error name, by default "internal"
	var errName string
	var errNameProvider svcerrors.WithName
	if errors.As(err, &errNameProvider) {
		errName = errNameProvider.ErrorName()
	} else {
		errName = "internal"
	}

	// User message, internal errors are masked by default
	var userMessages []string
	var userMsgProvider svcerrors.WithUserMessage
	switch {
	case errors.As(err, &userMsgProvider):
		formattedErr := userMsgProvider.ErrorUserMessage()
		userMessages = strings.Split(strings.TrimSpace(formattedErr), "\n")
	case status != http.StatusInternalServerError:
		userMessages = []string{"Internal Server Error Oops! Something went wrong."}
	default:
		formattedErr := errors.Format(err, errors.FormatAsSentences())
		userMessages = strings.Split(strings.TrimSpace(formattedErr), "\n")
	}

	// Log message
	var logMessage string
	var logMsgProvider svcerrors.WithLogMessage
	if errors.As(err, &logMsgProvider) {
		logMessage = logMsgProvider.ErrorLogMessage()
	} else {
		logMessage = errors.Format(err, errors.FormatWithUnwrap(), errors.FormatWithStack())
		if errName != "" {
			logMessage = errName + ": " + logMessage
		}
	}

	// Details, if it is not internal error, and there is a user message
	var details string
	if status != http.StatusInternalServerError && userMsgProvider != nil {
		details = errors.Format(err, errors.FormatAsSentences())
		if errName != "" {
			details = errName + ": " + details
		}
	}

	// ExceptionID
	var exceptionID string
	var exceptionIDProvider svcerrors.WithExceptionID
	if errors.As(err, &exceptionIDProvider) {
		exceptionID = exceptionIDProvider.ErrorExceptionId()
	} else {
		exceptionID = svcerrors.GenerateExceptionID()
	}

	// Log
	if status == http.StatusInternalServerError {
		pw.logger.Error(req.Context(), logMessage)
	} else {
		pw.logger.Warn(req.Context(), logMessage)
	}

	// Render page
	pw.WriteErrorPage(w, req, status, userMessages, details, exceptionID)
}

func (pw *Writer) WriteErrorPage(w http.ResponseWriter, req *http.Request, status int, messages []string, details, exceptionID string) {
	pw.writePage(w, req, "error.gohtml", status, &errorPageData{
		Status:      status,
		StatusText:  http.StatusText(status),
		Messages:    messages,
		Details:     details,
		ExceptionID: exceptionID,
	})
}
