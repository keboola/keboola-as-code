package model

// NonRetryableError wraps an error to indicate that the operation should not be retried.
// For example, expired credentials will never succeed on retry without external intervention.
type NonRetryableError struct {
	Err error
}

func NewNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{Err: err}
}

func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}
