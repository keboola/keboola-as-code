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

type Event struct {
	*op.KeyValue
	Type   EventType
	Header *etcdserverpb.ResponseHeader
}

type EventT[T any] struct {
	op.KeyValueT[T]
	Type   EventType
	Header *etcdserverpb.ResponseHeader
}

func (e *EventT[T]) Rev() int64 {
	return e.Header.Revision
}

func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, handleErr func(error), opts ...etcd.OpOption) <-chan Event {
	ch := make(chan Event)
	v.doWatch(ctx, client, handleErr, ch, opts...)
	return ch
}

func (v Prefix) doWatch(ctx context.Context, client etcd.Watcher, handleErr func(err error), ch chan Event, opts ...etcd.OpOption) {
	opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
	rawCh := client.Watch(ctx, v.Prefix(), opts...)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-rawCh:
				if !ok {
					// Close typed channel, if raw channel is closed
					close(ch)
					return
				}

				if err := resp.Err(); err != nil {
					handleErr(err)
					continue
				}

				for _, rawEvent := range resp.Events {
					outEvent := Event{KeyValue: rawEvent.Kv, Header: &resp.Header}

					// Map event type
					switch rawEvent.Type {
					case mvccpb.PUT:
						if rawEvent.Kv.CreateRevision == rawEvent.Kv.ModRevision {
							outEvent.Type = CreateEvent
						} else {
							outEvent.Type = UpdateEvent
						}
					case mvccpb.DELETE:
						outEvent.Type = DeleteEvent
					default:
						panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
					}

					ch <- outEvent
				}
			}
		}
	}()
}

func (v PrefixT[T]) Watch(ctx context.Context, client etcd.Watcher, handleErr func(error), opts ...etcd.OpOption) <-chan EventT[T] {
	ch := make(chan EventT[T])
	v.doWatch(ctx, client, handleErr, ch, opts...)
	return ch
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

func (v PrefixT[T]) doWatch(ctx context.Context, client etcd.Watcher, handleErr func(err error), typedCh chan EventT[T], opts ...etcd.OpOption) {
	rawCh := v.prefix.Watch(ctx, client, handleErr, opts...)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case rawEvent, ok := <-rawCh:
				if !ok {
					// Close typed channel, if raw channel is closed
					close(typedCh)
					return
				}

				outEvent := EventT[T]{
					KeyValueT: op.KeyValueT[T]{KV: rawEvent.KeyValue},
					Type:      rawEvent.Type,
					Header:    rawEvent.Header,
				}

				// We care for the value only in CREATE/UPDATE operation
				if rawEvent.Type == CreateEvent || rawEvent.Type == UpdateEvent {
					target := new(T)
					if err := v.serde.Decode(ctx, rawEvent.KeyValue, target); err != nil {
						handleErr(err)
						continue
					}
					outEvent.Value = *target
				}

				typedCh <- outEvent
			}
		}
	}()
}
