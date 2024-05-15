package op

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ErrorTxn wraps a static error as TxnOp.
// It makes error handling easier and move it to one place.
func ErrorTxn[R any](err error) *TxnOp[R] {
	return TxnWithResult[R](nil, nil).AddError(err)
}

// FirstErrorOnly will only keep the first error that occurred.
func (v *TxnOp[R]) FirstErrorOnly() *TxnOp[R] {
	return v.AddProcessor(func(_ context.Context, r *TxnResult[R]) {
		if multi, ok := r.Err().(errors.MultiError); ok { //nolint:errorlint // root error must be multi error, errors.As is not used
			if errs := multi.WrappedErrors(); len(errs) > 0 {
				r.ResetErr()
				r.AddErr(errs[0])
			}
		}
	})
}
