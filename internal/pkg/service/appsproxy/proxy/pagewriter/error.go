package pagewriter

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ExceptionIDPrefix = "keboola-appsproxy-"
)

type errorPageData struct {
	App         *AppData
	Status      int
	StatusText  string
	Details     string
	ExceptionID string
	errorCopy
}

// errorCopy is the plain-language version of an HTTP status, for the error page.
//
// The audience of this page is an end user of a data app, not the person who
// deployed it — often not a Keboola user at all. "502 Bad Gateway" tells them
// neither what happened nor what to do next, which is what UT-4805 reported.
// Title answers the first question, Guidance the second, and Retryable decides
// whether offering a retry would be honest: on a 403 it would only fail again.
//
// The status code and its text are still rendered, demoted to the reference
// block beside the exception ID, where support actually looks for them.
type errorCopy struct {
	Title     string
	Guidance  string
	Retryable bool
}

func copyForStatus(status int) errorCopy {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return errorCopy{
			Title:    "You don't have access to this app",
			Guidance: "Your account isn't allowed to open this application. Ask the person who shared it with you to give you access.",
		}

	case http.StatusNotFound:
		return errorCopy{
			Title:    "This app doesn't exist",
			Guidance: "The address may be mistyped, or the application may have been deleted.",
		}

	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return errorCopy{
			Title:     "The app is busy right now",
			Guidance:  "It received more requests than it could handle. Wait a few seconds, then try again.",
			Retryable: true,
		}

	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return errorCopy{
			Title:     "This app isn't responding",
			Guidance:  "The application is there, but it didn't answer in time. It may be restarting or under load — trying again usually works.",
			Retryable: true,
		}

	default:
		if status >= http.StatusInternalServerError {
			return errorCopy{
				Title:     "Something went wrong on our side",
				Guidance:  "This isn't caused by anything you did. Try again — if it keeps happening, send the exception ID below to support.",
				Retryable: true,
			}
		}
		return errorCopy{
			Title:    "This request couldn't be completed",
			Guidance: "The application couldn't handle the request. Check the address you opened, then try again.",
		}
	}
}

func (pw *Writer) ProxyErrorHandlerFor(app api.AppConfig) func(w http.ResponseWriter, req *http.Request, err error) {
	return func(w http.ResponseWriter, req *http.Request, err error) {
		pw.ProxyErrorHandler(w, req, app, err)
	}
}

func (pw *Writer) ProxyErrorHandler(w http.ResponseWriter, req *http.Request, app api.AppConfig, err error) {
	// The transport error names the internal upstream address (K8s service DNS
	// name, pod IP, backend port). The error page is rendered for anonymous
	// clients too, so the error is kept for the log only and replaced by a
	// generic one for the page.
	pw.WriteError(w, req, &app, svcerrors.
		NewBadGatewayError(errors.New("request to application failed")).
		WithUserMessage("Request to application failed.").
		WithLogMessage(errors.Format(err, errors.FormatWithUnwrap(), errors.FormatWithStack())))
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
	var userMessages string
	var userMsgProvider svcerrors.WithUserMessage
	switch {
	case errors.As(err, &userMsgProvider):
		formattedErr := userMsgProvider.ErrorUserMessage()
		userMessages = strings.TrimSpace(formattedErr)
	case status != http.StatusInternalServerError:
		userMessages = "Internal Server Error Oops! Something went wrong."
	default:
		formattedErr := errors.Format(err, errors.FormatAsSentences())
		userMessages = strings.TrimSpace(formattedErr)
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
			details = errName + ":\n" + details
		}
	}

	// Add user messages when details are absent or there is different message than details contains
	if details == "" {
		details = userMessages
		// Remove last character as it typically contains `.` character
	} else if !strings.Contains(details, userMessages[:len(userMessages)-1]) {
		details = userMessages + "\n" + details
	}

	// ExceptionID
	var exceptionID string
	var exceptionIDProvider svcerrors.WithExceptionID
	if errors.As(err, &exceptionIDProvider) {
		exceptionID = exceptionIDProvider.ErrorExceptionID()
	} else {
		exceptionID = svcerrors.GenerateExceptionID()
	}

	// Add exception id prefix (if the error is not from another service)
	if !strings.Contains(exceptionID, "keboola") {
		exceptionID = ExceptionIDPrefix + exceptionID
	}

	// Add attributes
	req = req.WithContext(ctxattr.ContextWith(
		req.Context(),
		semconv.HTTPStatusCode(status),
		attribute.String("exceptionId", exceptionID),
		attribute.String("error.userMessages", userMessages),
		attribute.String("error.details", details),
	))

	// Log
	if status == http.StatusInternalServerError {
		pw.logger.Error(req.Context(), logMessage)
	} else {
		pw.logger.Warn(req.Context(), logMessage)
	}

	// Render page
	pw.WriteErrorPage(w, req, app, status, details, exceptionID)
}

func (pw *Writer) WriteErrorPage(w http.ResponseWriter, req *http.Request, app *api.AppConfig, status int, details, exceptionID string) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")

	data := &errorPageData{
		Status:      status,
		StatusText:  http.StatusText(status),
		Details:     details,
		ExceptionID: exceptionID,
		errorCopy:   copyForStatus(status),
	}

	// App info is filled in for requests/errors related to an app, otherwise it is empty
	if app != nil {
		appData := NewAppData(app)
		data.App = &appData
	}

	pw.writePage(w, req, "error.gohtml", status, data)
}
