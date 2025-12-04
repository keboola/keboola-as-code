package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	ContextCodeRestartDisabled = "apps.restartDisabled"
)

// Error represents the structure of Sandboxes API error.
type Error struct {
	Message     string         `json:"error"`
	ExceptionID string         `json:"exceptionId"`
	Context     map[string]any `json:"context,omitempty"`
	request     *http.Request
	response    *http.Response
}

// UnmarshalJSON implements custom unmarshalling to handle cases where context is an empty array instead of an object.
func (e *Error) UnmarshalJSON(data []byte) error {
	type Alias Error
	aux := &struct {
		Context json.RawMessage `json:"context,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(e),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Handle context field - it can be an object, null, or sometimes an empty array
	if len(aux.Context) > 0 {
		// Check if it's an array (starts with '[')
		if aux.Context[0] == '[' {
			// It's an array, treat as empty context
			e.Context = nil
		} else {
			// It's an object or null, unmarshal normally
			if err := json.Unmarshal(aux.Context, &e.Context); err != nil {
				return err
			}
		}
	}

	return nil
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

func (e *Error) HasRestartDisabled() bool {
	if e.Context == nil {
		return false
	}
	contextCode, ok := e.Context["code"].(string)
	return ok && contextCode == ContextCodeRestartDisabled
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
