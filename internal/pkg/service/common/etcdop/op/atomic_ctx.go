package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

const (
	clientCtxKey = ctxKey("etcdClient")
)

type ctxKey string

func ctxWithClient(ctx context.Context, client etcd.KV) context.Context {
	return context.WithValue(ctx, clientCtxKey, client)
}

func ClientFromCtx(ctx context.Context) etcd.KV {
	return ctx.Value(clientCtxKey).(etcd.KV)
}
