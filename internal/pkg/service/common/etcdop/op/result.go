package op

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Result[R any] struct {
	response *RawResponse
	result   R
	errors   errors.MultiError
}

// TxnResult is result of the TxnOp.
type TxnResult struct {
	succeeded bool
	*txnResults
}

type txnResults = Result[[]any]

func newResult[R any](r *RawResponse) *Result[R] {
	return &Result[R]{
		errors:   errors.NewMultiError(),
		response: r,
	}
}

func newTxnResult(r *RawResponse) *TxnResult {
	out := &TxnResult{}
	out.txnResults = newResult[[]any](r)
	return out
}

func (v *Result[R]) Response() *RawResponse {
	return v.response
}

func (v *Result[R]) Header() *Header {
	return getResponseHeader(v.response)
}

func (v *Result[R]) Result() R {
	return v.result
}

func (v *Result[R]) Err() error {
	return v.errors.ErrorOrNil()
}

func (v *Result[R]) HeaderOrErr() (*Header, error) {
	return v.Header(), v.Err()
}

func (v *Result[R]) ResultOrErr() (R, error) {
	return v.result, v.Err()
}

func (v *Result[R]) SetResult(result R) *Result[R] {
	v.result = result
	return v
}

func (v *Result[R]) AddErr(err error) *Result[R] {
	v.errors.Append(err)
	return v
}

func (v *TxnResult) Succeeded() bool {
	return v.succeeded
}

func (v *TxnResult) AddResult(result any) *TxnResult {
	v.result = append(v.result, result)
	return v
}

func (v *TxnResult) AddErr(err error) *TxnResult {
	v.txnResults.AddErr(err)
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
