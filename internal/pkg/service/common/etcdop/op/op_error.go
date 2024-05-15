package op

import "context"

type errorOp struct {
	err error
}

// ErrorOp wraps a static error as Op.
// It makes error handling easier and move it to one place.
func ErrorOp(err error) Op {
	return &errorOp{err: err}
}

func (v *errorOp) Op(context.Context) (LowLevelOp, error) {
	return LowLevelOp{}, v.err
}
