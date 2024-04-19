package pagewriter

import (
	"net"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const ExceptionIDPrefix = "keboola-appsproxy-"

type errorPageData struct {
	App         *AppData
	Status      int
	StatusText  string
	Messages    []string
	Details     string
	ExceptionID string
}

func (pw *Writer) ProxyErrorHandlerFor(app api.AppConfig) func(w http.ResponseWriter, req *http.Request, err error) {
	return func(w http.ResponseWriter, req *http.Request, err error) {
		pw.ProxyErrorHandler(w, req, app, err)
	}
}

func (pw *Writer) ProxyErrorHandler(w http.ResponseWriter, req *http.Request, app api.AppConfig, err error) {
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		pw.logger.Info(req.Context(), "app is not running, rendering spinner page")
		pw.WriteSpinnerPage(w, req, app)
	}

	pw.WriteError(w, req, &app, svcerrors.NewBadGatewayError(err).WithUserMessage("Request to application failed."))
}

func (pw *Writer) WriteError(w http.ResponseWriter, req *http.Request, app *api.AppConfig, err error) {
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

	// Add exception id prefix (if the error is not from another service)
	if !strings.Contains(exceptionID, "-") {
		exceptionID = ExceptionIDPrefix + exceptionID
	}

	// Log
	if status == http.StatusInternalServerError {
		pw.logger.Error(req.Context(), logMessage)
	} else {
		pw.logger.Warn(req.Context(), logMessage)
	}

	// Render page
	pw.WriteErrorPage(w, req, app, status, userMessages, details, exceptionID)
}

func (pw *Writer) WriteErrorPage(w http.ResponseWriter, req *http.Request, app *api.AppConfig, status int, messages []string, details, exceptionID string) {
	data := &errorPageData{
		Status:      status,
		StatusText:  http.StatusText(status),
		Messages:    messages,
		Details:     details,
		ExceptionID: exceptionID,
	}

	// App info is filled in for requests/errors related to an app, otherwise it is empty
	if app != nil {
		appData := NewAppData(app)
		data.App = &appData
	}

	pw.writePage(w, req, "error.gohtml", status, data)
}
