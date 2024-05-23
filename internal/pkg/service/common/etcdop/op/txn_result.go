package op

// TxnResult is result of the TxnOp.
type TxnResult[R any] struct {
	*resultBase
	result *R
}

func newTxnResult[R any](base *resultBase, result *R) *TxnResult[R] {
	return &TxnResult[R]{resultBase: base, result: result}
}

func newErrorTxnResult[R any](err error) *TxnResult[R] {
	r := newTxnResult[R](newResultBase(nil), nil)
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
