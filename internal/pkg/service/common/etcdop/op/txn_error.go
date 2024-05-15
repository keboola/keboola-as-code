package op

// ErrorTxn wraps a static error as TxnOp.
// It makes error handling easier and move it to one place.
func ErrorTxn[R any](err error) *TxnOp[R] {
	return TxnWithResult[R](nil, nil).AddError(err)
}
