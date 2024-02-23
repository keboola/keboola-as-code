package etcdclient

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type watcherWrapper struct {
	client *etcd.Client
	prefix string
}

func NewWatcher(client *etcd.Client, prefix string) etcd.Watcher {
	return &watcherWrapper{
		client: client,
		prefix: prefix,
	}
}

func (w *watcherWrapper) Watch(ctx context.Context, key string, opts ...etcd.OpOption) etcd.WatchChan {
	// Workaround: create always a new watcher
	//   https://github.com/etcd-io/etcd/pull/14995
	//   https://github.com/etcd-io/etcd/issues/15058
	//   It's very easy for client application to workaround the issue.
	//   The client just needs to create a new client each time before watching.
	return namespace.NewWatcher(etcd.NewWatcher(w.client), w.prefix).Watch(ctx, key, opts...)
}

func (w *watcherWrapper) RequestProgress(_ context.Context) error {
	panic(errors.New("not implemented"))
}

func (w *watcherWrapper) Close() error {
	// NOP
	// Individual watchers must be terminated via the context
	return nil
}
