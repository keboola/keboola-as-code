package etcdop

import (
	"context"
	"strconv"
	"time"

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

// GetAllAndWatch loads all keys in the prefix by the iterator and then watch for changes.
// initDone channel signals end of the load phase and start of the watch phase.
func (v Prefix) GetAllAndWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) (out <-chan Event, initDone <-chan error) {
	outCh := make(chan Event)
	initDoneCh := make(chan error)

	go func() {
		// GetAll
		itr := v.GetAll().Do(ctx, client)
		err := itr.ForEach(func(kv *op.KeyValue, header *etcdserverpb.ResponseHeader) error {
			outCh <- Event{
				Kv:     kv,
				Type:   CreateEvent,
				Header: header,
			}
			return nil
		})
		if err != nil {
			// GetAll error is fatal
			handleErr(err)
			initDoneCh <- err
			close(outCh)
			close(initDoneCh)
			return
		}

		// Continue with Watch where GetAll ended
		close(initDoneCh)
		opts = append(opts, etcd.WithRev(itr.Header().Revision+1))
		v.doWatch(ctx, client, handleErr, outCh, opts...)
	}()

	return outCh, initDoneCh
}

func (v Prefix) doWatch(ctx context.Context, client etcd.Watcher, handleErr func(err error), outCh chan Event, opts ...etcd.OpOption) {
	opts = append([]etcd.OpOption{etcd.WithPrefix()}, opts...)
	go func() {
		defer close(outCh)

		// In case of an error, the watch channel can be closed.
		// It will be recreated, and it will continue from the last revision.
		revision := int64(0)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// If the watch is recreated, continue from the last received revision
				watchOpts := opts
				if revision > 0 {
					watchOpts = append(watchOpts, etcd.WithRev(revision+1))
				}

				// Wait before recreate attempt
				if revision > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
						// continue
					}
				}

				// Process watch events until the rawCh is closed
				rawCh := client.Watch(ctx, v.Prefix(), watchOpts...)
				processWatchEvents(ctx, &revision, handleErr, rawCh, outCh)
			}
		}
	}()
}

func (v PrefixT[T]) Watch(ctx context.Context, client etcd.Watcher, handleErr func(error), opts ...etcd.OpOption) <-chan EventT[T] {
	ch := make(chan EventT[T])
	v.doWatch(ctx, client, handleErr, ch, opts...)
	return ch
}

// GetAllAndWatch loads all keys in the prefix by the iterator and then watch for changes.
// Values are decoded to the type T.
// initDone channel signals end of the load phase and start of the watch phase.
func (v PrefixT[T]) GetAllAndWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) (out <-chan EventT[T], initDone <-chan error) {
	outCh := make(chan EventT[T])
	initDoneCh := make(chan error)

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
			initDoneCh <- err
			close(outCh)
			close(initDoneCh)
			return
		}

		// Continue with Watch where GetAll finished
		close(initDoneCh)
		opts = append(opts, etcd.WithRev(itr.Header().Revision+1))
		v.doWatch(ctx, client, handleErr, outCh, opts...)
	}()

	return outCh, initDoneCh
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

				if rawEvent.Type == CreateEvent || rawEvent.Type == UpdateEvent {
					// Always decode create/update value
					target := new(T)
					if err := v.serde.Decode(ctx, rawEvent.Kv, target); err != nil {
						handleErr(err)
						continue
					}
					outEvent.Value = *target
				} else if rawEvent.Type == DeleteEvent && rawEvent.PrevKv != nil {
					// Decode previous value on delete, if is present.
					// etcd.WithPrevKV() option must be used to enable it.
					target := new(T)
					if err := v.serde.Decode(ctx, rawEvent.PrevKv, target); err != nil {
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

func processWatchEvents(ctx context.Context, revision *int64, handleErr func(err error), rawCh etcd.WatchChan, outCh chan<- Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-rawCh:
			if !ok {
				return
			}

			*revision = resp.Header.Revision

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

				outCh <- outEvent
			}
		}
	}
}
