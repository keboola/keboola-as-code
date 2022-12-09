package etcdop

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type EventType int32

const (
	CreateEvent EventType = iota
	UpdateEvent
	DeleteEvent
)

type EventT[T any] struct {
	op.KeyValueT[T]
	Type   EventType
	Header *etcdserverpb.ResponseHeader
}

func (e *EventT[T]) Rev() int64 {
	return e.Header.Revision
}

func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) etcd.WatchChan {
	opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
	return client.Watch(ctx, v.Prefix(), opts...)
}

func (v PrefixT[T]) Watch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) <-chan EventT[T] {
	typedCh := make(chan EventT[T])
	v.doWatch(ctx, client, handleErr, typedCh, opts...)
	return typedCh
}

func (v PrefixT[T]) GetAllAndWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) <-chan EventT[T] {
	typedCh := make(chan EventT[T])

	go func() {
		// GetAll
		itr := v.GetAll().Do(ctx, client)
		err := itr.ForEachKV(func(kv op.KeyValueT[T], header *etcdserverpb.ResponseHeader) error {
			typedCh <- EventT[T]{
				KeyValueT: kv,
				Type:      CreateEvent,
				Header:    header,
			}
			return nil
		})
		if err != nil {
			// GetAll error is fatal
			handleErr(err)
			close(typedCh)
		}

		// Continue with Watch where GetAll ended
		opts = append(opts, etcd.WithRev(itr.Header().Revision+1))
		v.doWatch(ctx, client, handleErr, typedCh, opts...)
	}()

	return typedCh
}

func (v PrefixT[T]) doWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), typedCh chan EventT[T], opts ...etcd.OpOption) {
	rawCh := v.prefix.Watch(ctx, client, opts...)
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

				if err := resp.Err(); err != nil {
					handleErr(err)
					continue
				}

				for _, event := range resp.Events {
					typedEvent := EventT[T]{KeyValueT: op.KeyValueT[T]{KV: event.Kv}, Header: &resp.Header}

					// Map event type
					switch event.Type {
					case mvccpb.PUT:
						if event.Kv.CreateRevision == event.Kv.ModRevision {
							typedEvent.Type = CreateEvent
						} else {
							typedEvent.Type = UpdateEvent
						}
					case mvccpb.DELETE:
						typedEvent.Type = DeleteEvent
					default:
						panic(errors.Errorf(`unexpected event type "%s"`, event.Type.String()))
					}

					// We care for the value only in PUT operation
					if event.Type == mvccpb.PUT {
						target := new(T)
						if err := v.serde.Decode(ctx, event.Kv, target); err != nil {
							handleErr(err)
							continue
						}
						typedEvent.Value = *target
					}
					typedCh <- typedEvent
				}
			}
		}
	}()
}
