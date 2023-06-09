package telemetry

import (
	"context"
)

const disabledTracingCtxKey = ctxKey("disabled-tracing")

func ContextWithDisabledTracing(ctx context.Context) context.Context {
	return context.WithValue(ctx, disabledTracingCtxKey, true)
}

func IsTracingDisabled(ctx context.Context) bool {
	v, _ := ctx.Value(disabledTracingCtxKey).(bool)
	return v
}
