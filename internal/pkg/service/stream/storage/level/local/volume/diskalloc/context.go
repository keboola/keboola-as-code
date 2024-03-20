package diskalloc

import "context"

const configCtxKey = ctxKey("diskalloc-config")

type ctxKey string

func ContextWithConfig(ctx context.Context, cfg Config) context.Context {
	return context.WithValue(ctx, configCtxKey, cfg)
}

func ConfigFromContext(ctx context.Context) (Config, bool) {
	v, ok := ctx.Value(configCtxKey).(Config)
	return v, ok
}
