package task

import "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"

type TaskLockError struct {
	error
}

// UserError marks the wrapped error as expected, so it will not be taken as an error in the metrics.
// UserError is an error that could occur during normal operation.
type UserError struct {
	error
}

func (e UserError) Unwrap() error {
	return e.error
}

// WrapUserError marks the error as the UserError, so it will not be taken as an error in the metrics.
// UserError is an error that could occur during normal operation.
func WrapUserError(err error) error {
	return &UserError{error: err}
}

func isUserError(err error) bool {
	var expectedErr *UserError
	return errors.As(err, &expectedErr)
}

func (e TaskLockError) Unwrap() error {
	return e.error
}
