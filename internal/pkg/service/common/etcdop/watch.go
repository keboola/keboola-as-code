package etcdop

import (
	"context"
	"strconv"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type EventType int

const (
	CreateEvent EventType = iota
	UpdateEvent
	DeleteEvent
)

type Event struct {
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Type   EventType
	Header *etcdserverpb.ResponseHeader
}

type EventT[T any] struct {
	Value  T
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Type   EventType
	Header *etcdserverpb.ResponseHeader
}

func (v EventType) String() string {
	switch v {
	case CreateEvent:
		return "create"
	case UpdateEvent:
		return "update"
	case DeleteEvent:
		return "delete"
	default:
		return strconv.Itoa(int(v))
	}
}

func (e *EventT[T]) Rev() int64 {
	return e.Header.Revision
}

func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, handleErr func(error), opts ...etcd.OpOption) <-chan Event {
	ch := make(chan Event)
	v.doWatch(ctx, client, handleErr, ch, opts...)
	return ch
}

func (v Prefix) GetAllAndWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) <-chan Event {
	ch := make(chan Event)

	go func() {
		// GetAll
		itr := v.GetAll().Do(ctx, client)
		err := itr.ForEach(func(kv *op.KeyValue, header *etcdserverpb.ResponseHeader) error {
			ch <- Event{
				Kv:     kv,
				Type:   CreateEvent,
				Header: header,
			}
			return nil
		})
		if err != nil {
			// GetAll error is fatal
			handleErr(err)
			close(ch)
		}

		// Continue with Watch where GetAll ended
		opts = append(opts, etcd.WithRev(itr.Header().Revision+1))
		v.doWatch(ctx, client, handleErr, ch, opts...)
	}()

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
					// Close output channel, if raw channel is closed
					close(ch)
					return
				}

				if err := resp.Err(); err != nil {
					handleErr(err)
					continue
				}

				for _, rawEvent := range resp.Events {
					outEvent := Event{Kv: rawEvent.Kv, PrevKv: rawEvent.PrevKv, Header: &resp.Header}

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
	outCh := make(chan EventT[T])

	go func() {
		// GetAll
		itr := v.GetAll().Do(ctx, client)
		err := itr.ForEachKV(func(kv op.KeyValueT[T], header *etcdserverpb.ResponseHeader) error {
			outCh <- EventT[T]{
				Kv:     kv.Kv,
				Value:  kv.Value,
				Type:   CreateEvent,
				Header: header,
			}
			return nil
		})
		if err != nil {
			// GetAll error is fatal
			handleErr(err)
			close(outCh)
		}

		// Continue with Watch where GetAll ended
		opts = append(opts, etcd.WithRev(itr.Header().Revision+1))
		v.doWatch(ctx, client, handleErr, outCh, opts...)
	}()

	return outCh
}

func (v PrefixT[T]) doWatch(ctx context.Context, client etcd.Watcher, handleErr func(err error), outCh chan EventT[T], opts ...etcd.OpOption) {
	rawCh := v.prefix.Watch(ctx, client, handleErr, opts...)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case rawEvent, ok := <-rawCh:
				if !ok {
					// Close output channel, if raw channel is closed
					close(outCh)
					return
				}

				outEvent := EventT[T]{
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
					Type:   rawEvent.Type,
					Header: rawEvent.Header,
				}

				// We care for the value only in CREATE/UPDATE operation
				if rawEvent.Type == CreateEvent || rawEvent.Type == UpdateEvent {
					target := new(T)
					if err := v.serde.Decode(ctx, rawEvent.Kv, target); err != nil {
						handleErr(err)
						continue
					}
					outEvent.Value = *target
				}

				outCh <- outEvent
			}
		}
	}()
}
