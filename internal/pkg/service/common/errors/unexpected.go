package errors

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

func (e UnexpectedError) Error() string {
	return e.Message
}

func (e UnexpectedError) ErrorName() string {
	return e.Name
}
