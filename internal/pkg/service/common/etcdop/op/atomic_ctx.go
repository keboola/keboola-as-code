package op

import (
	"context"
	"errors"

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
	client, ok := ctx.Value(clientCtxKey).(etcd.KV)
	if !ok {
		panic(errors.New("etcd client is not set"))
	}
	return client
}
