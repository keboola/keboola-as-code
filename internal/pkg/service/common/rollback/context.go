package rollback

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const rollbackCtxKey = ctxKey("rollback")

type ctxKey string

func ContextWith(ctx context.Context, rb Builder) context.Context {
	if _, ok := ctx.Value(rollbackCtxKey).(Builder); ok {
		panic(errors.Errorf("rollback builder is already present in the context"))
	}
	return context.WithValue(ctx, rollbackCtxKey, rb)
}

func FromContext(ctx context.Context) Builder {
	if builder, ok := ctx.Value(rollbackCtxKey).(Builder); ok {
		return builder
	} else {
		panic(errors.Errorf("rollback builder is not present in the context"))
	}
}
