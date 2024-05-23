package op

import "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"

// TxnResult is result of the TxnOp.
type TxnResult[R any] struct {
	*txnResultCore
	result *R
}

type txnResultCore struct {
	*resultCore
	subResults []any
}

func newTxnResult[R any](response *RawResponse, result *R) *TxnResult[R] {
	return &TxnResult[R]{
		txnResultCore: newTxnResultCore(response),
		result:        result,
	}
}

func newTxnResultCore(response *RawResponse) *txnResultCore {
	if response != nil && response.Txn() == nil {
		panic(errors.New("unexpected response"))
	}
	return &txnResultCore{
		resultCore: newResultCore(response),
	}
}

func newErrorTxnResult[R any](err error) *TxnResult[R] {
	r := newTxnResult[R](nil, nil)
	r.AddErr(err)
	return r
}

func (v *TxnResult[R]) Succeeded() bool {
	return v.response != nil && v.response.Txn().Succeeded
}

func (v *TxnResult[R]) ResultOrErr() (R, error) {
	if err := v.Err(); err == nil {
		return v.Result(), nil
	} else {
		var empty R
		return empty, err
	}
}

func (v *TxnResult[R]) Result() R {
	if v.result == nil {
		var empty R
		return empty
	} else {
		return *v.result
	}
}

func (v *TxnResult[R]) SubResults() []any {
	return v.subResults
}

func (v *TxnResult[R]) SetSubResults(results []any) {
	v.subResults = results
}

func (v *TxnResult[R]) AddSubResult(result any) {
	v.subResults = append(v.subResults, result)
}
