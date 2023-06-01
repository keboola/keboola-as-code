package task

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Result struct {
	result    string
	error     error
	errorType string
	// applicationError flag is true if the error is an unexpected error that should not occur during normal operation
	applicationError bool
	outputs          map[string]any
}

// UserError marks the wrapped error as expected, so it will not be taken as an error in the metrics.
// UserError is an error that could occur during normal operation.
type UserError struct {
	error
}

func (e UserError) Unwrap() error {
	return e.error
}

func OkResult(msg string) Result {
	if strings.TrimSpace(msg) == "" {
		panic(errors.New("message cannot be empty"))
	}
	return Result{result: msg}
}

func ErrResult(err error) Result {
	if err == nil {
		panic(errors.New("error cannot be nil"))
	}
	return (Result{}).withError(err)
}

// WrapUserError marks the error as the UserError, so it will not be taken as an error in the metrics.
// UserError is an error that could occur during normal operation.
func WrapUserError(err error) error {
	return &UserError{error: err}
}

func (r Result) Result() string {
	return r.result
}

func (r Result) Error() error {
	return r.error
}

func (r Result) ErrorType() string {
	return r.errorType
}

func (r Result) IsError() bool {
	return r.error != nil
}

// IsUserError returns true if the error is an expected error that could occur during normal operation.
func (r Result) IsUserError() bool {
	return !r.applicationError
}

// IsApplicationError returns true if the error is an unexpected error that should not occur during normal operation.
func (r Result) IsApplicationError() bool {
	return r.applicationError
}

// WithResult can be used to edit the result message later.
func (r Result) WithResult(result string) Result {
	if r.error == nil && r.result == "" {
		panic(errors.New(`result struct is empty, use task.OkResult(msg) or task.ErrResult(err) function instead`))
	}
	if r.error != nil {
		panic(errors.New(`result type is "error", not "ok", it cannot be modified`))
	}
	r.result = result
	return r
}

// WithError can be used to edit the error message later.
func (r Result) WithError(err error) Result {
	if r.error == nil && r.result == "" {
		panic(errors.New(`result struct is empty, use task.OkResult(msg) or task.ErrResult(err) function instead`))
	}
	if r.error == nil {
		panic(errors.New(`result type is "ok", not "error", it cannot be modified`))
	}
	return r.withError(err)
}

// WithOutput adds some task operation output.
func (r Result) WithOutput(k string, v any) Result {
	if r.error == nil && r.result == "" {
		panic(errors.New(`result struct is empty, use task.OkResult(msg) or task.ErrResult(err) function first`))
	}

	// Clone map
	original := r.outputs
	r.outputs = make(map[string]any)
	for key, value := range original {
		r.outputs[key] = value
	}

	// Add new key
	r.outputs[k] = v
	return r
}

func (r Result) withError(err error) Result {
	r.error = err
	r.errorType = telemetry.ErrorType(err)
	r.applicationError = isApplicationError(err)
	return r
}

func isApplicationError(err error) bool {
	var expectedErr *UserError
	return err != nil && !errors.As(err, &expectedErr)
}
