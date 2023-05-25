package task

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Result struct {
	result          string
	error           error
	errorType       string
	unexpectedError bool
	outputs         map[string]any
}

// ExpectedError marks the wrapped error as expected, so it will not be taken as an error in the metrics.
type ExpectedError struct {
	error
}

func (e ExpectedError) Unwrap() error {
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

// WrapExpectedError marks the error as expected, so it will not be taken as an error in the metrics.
func WrapExpectedError(err error) error {
	return &ExpectedError{error: err}
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

// IsUnexpectedError returns true if the error should be taken as an error in the metrics.
func (r Result) IsUnexpectedError() bool {
	return r.unexpectedError
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
	r.unexpectedError = isUnexpectedError(err)
	return r
}

func isUnexpectedError(err error) bool {
	var expectedErr *ExpectedError
	return err != nil && !errors.As(err, &expectedErr)
}
