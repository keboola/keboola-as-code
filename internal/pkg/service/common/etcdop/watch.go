package etcdop

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

type EventT[T any] struct {
	*etcd.Event
	Header *etcdserverpb.ResponseHeader
	Value  *T
}

func (e *EventT[T]) Rev() int64 {
	return e.Header.Revision
}

func (e *EventT[T]) Key() string {
	return string(e.Kv.Key)
}

func (v Prefix) Watch(ctx context.Context, client *etcd.Client, opts ...etcd.OpOption) etcd.WatchChan {
	opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
	return client.Watch(ctx, v.Prefix(), opts...)
}

func (v PrefixT[T]) Watch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) <-chan EventT[T] {
	rawCh := v.prefix.Watch(ctx, client, opts...)
	typedCh := make(chan EventT[T])

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-rawCh:
				if !ok {
					// Close typed channel, if raw channel is closed
					close(typedCh)
					return
				}

				for _, event := range resp.Events {
					typedEvent := EventT[T]{
						Event:  event,
						Header: &resp.Header,
					}

					// We care for the value only in PUT operation
					if event.Type == mvccpb.PUT {
						target := new(T)
						if err := v.serde.Decode(ctx, event.Kv, target); err != nil {
							handleErr(err)
							continue
						}
						typedEvent.Value = target
					}
					typedCh <- typedEvent
				}
			}
		}
	}()

	return typedCh
}
