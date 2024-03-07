package appconfig

import (
	"fmt"
	"net/http"
)

// SandboxesError represents the structure of API error.
type SandboxesError struct {
	Message     string `json:"error"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e *SandboxesError) Error() string {
	return fmt.Sprintf("sandboxes api error[%d]: %s", e.StatusCode(), e.Message)
}

// ErrorName returns a human-readable name of the error.
func (e *SandboxesError) ErrorName() string {
	return http.StatusText(e.StatusCode())
}

// ErrorUserMessage returns error message for end user.
func (e *SandboxesError) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *SandboxesError) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e *SandboxesError) StatusCode() int {
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *SandboxesError) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *SandboxesError) SetResponse(response *http.Response) {
	e.response = response
}
