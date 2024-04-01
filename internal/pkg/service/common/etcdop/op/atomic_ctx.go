package op

import (
	"context"
	"errors"
)

const (
	atomicOpCtxKey = ctxKey("atomicOp")
)

type ctxKey string

func ctxWithAtomicOp(ctx context.Context, op *AtomicOpCore) context.Context {
	return context.WithValue(ctx, atomicOpCtxKey, op)
}

func AtomicOpFromCtx(ctx context.Context) *AtomicOpCore {
	value, ok := ctx.Value(atomicOpCtxKey).(*AtomicOpCore)
	if !ok {
		panic(errors.New("atomic op is not set"))
	}
	return value
}
