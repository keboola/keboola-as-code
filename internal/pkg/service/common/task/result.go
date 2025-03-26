package task

import (
	"maps"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Result struct {
	Result  string
	Error   error
	Outputs Outputs
}

func OkResult(msg string) Result {
	if strings.TrimSpace(msg) == "" {
		panic(errors.New("message cannot be empty"))
	}
	return Result{Result: msg}
}

func ErrResult(err error) Result {
	if err == nil {
		panic(errors.New("error cannot be nil"))
	}
	return (Result{}).withError(err)
}

func (r Result) IsError() bool {
	if r.Error != nil && r.Result != "" {
		panic(errors.New("both Error and Result cannot be set"))
	}
	return r.Error != nil
}

// WithResult can be used to edit the result message later.
func (r Result) WithResult(result string) Result {
	if r.Error == nil && r.Result == "" {
		panic(errors.New(`result struct is empty, use task.OkResult(msg) or task.ErrResult(err) function instead`))
	}
	if r.Error != nil {
		panic(errors.New(`result type is "error", not "ok", it cannot be modified`))
	}
	r.Result = result
	return r
}

// WithError can be used to edit the error message later.
func (r Result) WithError(err error) Result {
	if r.Error == nil && r.Result == "" {
		panic(errors.New(`result struct is empty, use task.OkResult(msg) or task.ErrResult(err) function instead`))
	}
	if r.Error == nil {
		panic(errors.New(`result type is "ok", not "error", it cannot be modified`))
	}
	return r.withError(err)
}

// WithOutput adds an operation output the task.
func (r Result) WithOutput(k string, v any) Result {
	if r.Error == nil && r.Result == "" {
		panic(errors.New(`result struct is empty, use task.OkResult(msg) or task.ErrResult(err) function first`))
	}

	// Clone map
	original := r.Outputs
	r.Outputs = make(map[string]any)
	maps.Copy(r.Outputs, original)

	// Add new key
	r.Outputs[k] = v
	return r
}

// WithOutputsFrom adds operation outputs the task from a struct.
func (r Result) WithOutputsFrom(v any) Result {
	// Clone map
	original := r.Outputs
	r.Outputs = make(map[string]any)
	maps.Copy(r.Outputs, original)

	// Convert value to map
	m := make(map[string]any)
	if err := mapstructure.Decode(v, &m); err != nil {
		panic(err)
	}
	for key, value := range m {
		// Skip nil values
		if v := reflect.ValueOf(value); !v.IsValid() || (v.Kind() == reflect.Pointer && v.IsZero()) {
			continue
		}
		r.Outputs[key] = value
	}

	return r
}

func (r Result) withError(err error) Result {
	r.Error = err
	return r
}
