package op

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Result[R any] struct {
	response *RawResponse
	errors   errors.MultiError
	result   *R
}

// EmptyResultError signals an empty result of the etcd operation.
// See also WithResult.WithEmptyResultAsError method.
type EmptyResultError struct {
	error
}

func (v EmptyResultError) Unwrap() error {
	return v.error
}

func NewEmptyResultError(err error) EmptyResultError {
	return EmptyResultError{error: err}
}

func newResult[R any](r *RawResponse, result *R) *Result[R] {
	return &Result[R]{
		errors:   errors.NewMultiError(),
		response: r,
		result:   result,
	}
}

func newErrorResult[R any](err error) *Result[R] {
	return newResult[R](nil, nil).AddErr(err)
}

func (v *Result[R]) Response() *RawResponse {
	return v.response
}

func (v *Result[R]) Header() *Header {
	return getResponseHeader(v.response)
}

func (v *Result[R]) Result() R {
	if v.result == nil {
		var empty R
		return empty
	} else {
		return *v.result
	}
}

func (v *Result[R]) Err() error {
	return v.errors.ErrorOrNil()
}

func (v *Result[R]) HeaderOrErr() (*Header, error) {
	return v.Header(), v.Err()
}

func (v *Result[R]) ResultOrErr() (R, error) {
	if err := v.Err(); err == nil {
		return v.Result(), nil
	} else {
		var empty R
		return empty, err
	}
}

func (v *Result[R]) SetResult(result *R) *Result[R] {
	v.result = result
	return v
}

func (v *Result[R]) ResetErr() *Result[R] {
	v.errors = errors.NewMultiError()
	return v
}

func (v *Result[R]) AddErr(err error) *Result[R] {
	v.errors.Append(err)
	return v
}

func getResponseHeader(response *RawResponse) *Header {
	if response == nil {
		return nil
	} else if v := response.Get(); v != nil {
		return v.Header
	} else if v := response.Del(); v != nil {
		return v.Header
	} else if v := response.Put(); v != nil {
		return v.Header
	} else if v := response.Txn(); v != nil {
		return v.Header
	} else {
		return nil
	}
}
