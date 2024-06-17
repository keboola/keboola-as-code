package op

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Result of a high-level operation.
type Result[R any] struct {
	*resultBase
	result *R
}

// resultBase is common part of all results regardless of the type of value or whether it is a transaction.
type resultBase struct {
	response *RawResponse
	errors   errors.MultiError
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

func newResult[R any](response *RawResponse, result *R) *Result[R] {
	return &Result[R]{resultBase: newResultBase(response), result: result}
}

func newResultBase(response *RawResponse) *resultBase {
	return &resultBase{
		errors:   errors.NewMultiError(),
		response: response,
	}
}

func newErrorResult[R any](err error) *Result[R] {
	r := newResult[R](nil, nil)
	r.AddErr(err)
	return r
}

func (v *Result[R]) ResultOrErr() (R, error) {
	if err := v.Err(); err == nil {
		return v.Result(), nil
	} else {
		var empty R
		return empty, err
	}
}

func (v *Result[R]) Result() R {
	if v.result == nil {
		var empty R
		return empty
	} else {
		return *v.result
	}
}

func (v *resultBase) Response() *RawResponse {
	return v.response
}

func (v *resultBase) Header() *Header {
	return getResponseHeader(v.response)
}

func (v *resultBase) Err() error {
	return v.errors.ErrorOrNil()
}

func (v *resultBase) HeaderOrErr() (*Header, error) {
	return v.Header(), v.Err()
}

func (v *resultBase) ResetErr() {
	v.errors = errors.NewMultiError()
}

func (v *resultBase) AddErr(err error) {
	v.errors.Append(err)
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
