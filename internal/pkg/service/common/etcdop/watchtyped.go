package etcdop

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

type EventT[T any] struct {
	Value  T
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Type   EventType
}

type EventsT[T any] struct {
	Header *etcdserverpb.ResponseHeader
	Events []EventT[T]
	// Created is used to indicate the creation of the watcher.
	Created bool
	// InitErr signals an error during initialization of the watcher.
	InitErr error
}

func (e *EventsT[T]) Rev() int64 {
	return e.Header.Revision
}

func (v PrefixT[T]) Watch(ctx context.Context, client etcd.Watcher, handleErr func(error), opts ...etcd.OpOption) <-chan EventsT[T] {
	outCh := make(chan EventsT[T])
	go func() {
		defer close(outCh)
		v.doWatch(ctx, client, handleErr, outCh, opts...)
	}()
	return outCh
}

// GetAllAndWatch loads all keys in the prefix by the iterator and then watch for changes.
// Values are decoded to the type T.
// initDone channel signals end of the load phase and start of the watch phase.
func (v PrefixT[T]) GetAllAndWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) (out <-chan EventsT[T]) {
	outCh := make(chan EventsT[T])

	go func() {
		defer close(outCh)

		// Get all iterator
		itr := v.GetAll().Do(ctx, client)
		var events []EventT[T]
		sendBatch := func() {
			if len(events) > 0 {
				outCh <- EventsT[T]{Header: itr.Header(), Events: events}
			}
			events = nil
		}

		// Iterate and send batches of events
		i := 1
		err := itr.ForEachKV(func(kv op.KeyValueT[T], _ *etcdserverpb.ResponseHeader) error {
			events = append(events, EventT[T]{Kv: kv.Kv, Value: kv.Value, Type: CreateEvent})
			if i%getAllBatchSize == 0 {
				sendBatch()
			}
			return nil
		})
		sendBatch()

		// Check getAll error
		if err != nil {
			outCh <- EventsT[T]{InitErr: err}
			return
		}

		// Continue with Watch where GetAll finished
		v.doWatch(ctx, client, handleErr, outCh, append(opts, etcd.WithRev(itr.Header().Revision+1))...)
	}()

	return outCh
}

// doWatch is called from the Watch and GetAllAndWatch methods.
func (v PrefixT[T]) doWatch(ctx context.Context, client etcd.Watcher, handleErr func(err error), outCh chan EventsT[T], opts ...etcd.OpOption) {
	rawCh := v.prefix.Watch(ctx, client, handleErr, opts...)
	for {
		select {
		case <-ctx.Done():
			return
		case rawEvents, ok := <-rawCh:
			if !ok {
				return
			}

			outEvents := make([]EventT[T], len(rawEvents.Events))
			for i, rawEvent := range rawEvents.Events {
				outEvent := EventT[T]{
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
					Type:   rawEvent.Type,
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

				outEvents[i] = outEvent
			}

			outCh <- EventsT[T]{
				Header:  rawEvents.Header,
				Events:  outEvents,
				Created: rawEvents.Created,
				InitErr: rawEvents.InitErr,
			}
		}
	}
}
