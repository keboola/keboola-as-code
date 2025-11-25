package api

import (
	"fmt"
	"net/http"
)

// Error represents the structure of Sandboxes API error.
type Error struct {
	Message     string         `json:"error"`
	ExceptionID string         `json:"exceptionId"`
	Context     map[string]any `json:"context,omitempty"`
	request     *http.Request
	response    *http.Response
}

func (e *Error) Error() string {
	return fmt.Sprintf("sandboxes service error[%d]: %s", e.StatusCode(), e.Message)
}

// ErrorName returns a human-readable name of the error.
func (e *Error) ErrorName() string {
	return http.StatusText(e.StatusCode())
}

// ErrorUserMessage returns error message for end user.
func (e *Error) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *Error) ErrorExceptionID() string {
	return e.ExceptionID
}

func (e *Error) HasRestartDisabled(code string) bool {
	if e.Context == nil {
		return false
	}
	contextCode, ok := e.Context["code"].(string)
	return ok && contextCode == code
}

// StatusCode returns HTTP status code.
func (e *Error) StatusCode() int {
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *Error) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *Error) SetResponse(response *http.Response) {
	e.response = response
}
