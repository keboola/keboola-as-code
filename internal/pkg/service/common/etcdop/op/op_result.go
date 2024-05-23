package op

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Result[R any] struct {
	*resultCore
	result *R
}

type resultCore struct {
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
	return &Result[R]{
		resultCore: newResultCore(response),
		result:     result,
	}
}

func newResultCore(response *RawResponse) *resultCore {
	return &resultCore{
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

func (v *resultCore) Response() *RawResponse {
	return v.response
}

func (v *resultCore) Header() *Header {
	return getResponseHeader(v.response)
}

func (v *resultCore) Err() error {
	return v.errors.ErrorOrNil()
}

func (v *resultCore) HeaderOrErr() (*Header, error) {
	return v.Header(), v.Err()
}

func (v *resultCore) ResetErr() {
	v.errors = errors.NewMultiError()
}

func (v *resultCore) AddErr(err error) {
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
