package op

// TxnResult is result of the TxnOp.
type TxnResult[R any] struct {
	result     *Result[R]
	succeeded  bool
	subResults []any
}

func newTxnResult[R any](response *RawResponse, result *R) *TxnResult[R] {
	return &TxnResult[R]{
		result: newResult[R](response, result),
	}
}

func newErrorTxnResult[R any](err error) *TxnResult[R] {
	return newTxnResult[R](nil, nil).AddErr(err)
}

func (v *TxnResult[R]) Succeeded() bool {
	return v.succeeded
}

func (v *TxnResult[R]) SetSubResults(results []any) *TxnResult[R] {
	v.subResults = results
	return v
}

func (v *TxnResult[R]) SubResults() []any {
	return v.subResults
}

func (v *TxnResult[R]) AddSubResult(result any) *TxnResult[R] {
	v.subResults = append(v.subResults, result)
	return v
}

func (v *TxnResult[R]) Response() *RawResponse {
	return v.result.Response()
}

func (v *TxnResult[R]) Header() *Header {
	return v.result.Header()
}

func (v *TxnResult[R]) Result() R {
	return v.result.Result()
}

func (v *TxnResult[R]) Err() error {
	return v.result.Err()
}

func (v *TxnResult[R]) HeaderOrErr() (*Header, error) {
	return v.result.HeaderOrErr()
}

func (v *TxnResult[R]) ResultOrErr() (R, error) {
	return v.result.ResultOrErr()
}

func (v *TxnResult[R]) SetResult(result *R) *TxnResult[R] {
	v.result.SetResult(result)
	return v
}

func (v *TxnResult[R]) ResetErr() *TxnResult[R] {
	v.result.ResetErr()
	return v
}

func (v *TxnResult[R]) AddErr(err error) *TxnResult[R] {
	v.result.AddErr(err)
	return v
}
