package etcdop

import (
	"context"
	"sort"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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

const (
	// getAllBatchSize defines batch size for "getAll" phase of the GetAllAndWatch operation.
	getAllBatchSize = 100
)

type Event struct {
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Type   EventType
}

type Events struct {
	Header *etcdserverpb.ResponseHeader
	Events []Event
	// Created is used to indicate the creation of the watcher.
	Created bool
	// InitErr signals an error during initialization of the watcher.
	InitErr error
}

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

func (e *Events) Rev() int64 {
	return e.Header.Revision
}

func (e *EventsT[T]) Rev() int64 {
	return e.Header.Revision
}

func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, handleErr func(error), opts ...etcd.OpOption) <-chan Events {
	outCh := make(chan Events)
	go func() {
		defer close(outCh)
		v.doWatch(ctx, client, handleErr, outCh, opts...)
	}()
	return outCh
}

// GetAllAndWatch loads all keys in the prefix by the iterator and then watch for changes.
// initDone channel signals end of the load phase and start of the watch phase.
func (v Prefix) GetAllAndWatch(ctx context.Context, client *etcd.Client, handleErr func(err error), opts ...etcd.OpOption) (out <-chan Events) {
	outCh := make(chan Events)

	go func() {
		defer close(outCh)

		// Get all iterator
		itr := v.GetAll().Do(ctx, client)
		var events []Event
		sendBatch := func() {
			if len(events) > 0 {
				outCh <- Events{Header: itr.Header(), Events: events}
			}
			events = nil
		}

		// Iterate and send batches of events
		i := 1
		err := itr.ForEach(func(kv *op.KeyValue, _ *etcdserverpb.ResponseHeader) error {
			events = append(events, Event{Kv: kv, Type: CreateEvent})
			if i%getAllBatchSize == 0 {
				sendBatch()
			}
			return nil
		})
		sendBatch()

		// Check getAll error
		if err != nil {
			outCh <- Events{InitErr: err}
			return
		}

		// Continue with Watch where GetAll ended
		v.doWatch(ctx, client, handleErr, outCh, append(opts, etcd.WithRev(itr.Header().Revision+1))...)
	}()

	return outCh
}

func (v Prefix) doWatch(ctx context.Context, client etcd.Watcher, handleErr func(err error), outCh chan Events, opts ...etcd.OpOption) {
	opts = append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCreatedNotify()}, opts...)

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
				case <-time.After(10 * time.Second):
					// continue
				}
			}

			// Process watch events until the rawCh is closed
			rawCh := client.Watch(ctx, v.Prefix(), watchOpts...)
			retry, err := processWatchEvents(ctx, &revision, handleErr, rawCh, outCh)
			if err != nil {
				if errors.Is(err, rpctypes.ErrCompacted) {
					revision = 0
				}
			}
			if !retry {
				return
			}
		}
	}
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

func processWatchEvents(ctx context.Context, revision *int64, handleErr func(err error), rawCh etcd.WatchChan, outCh chan<- Events) (retry bool, err error) {
	created := false
	for {
		select {
		case <-ctx.Done():
			return false, nil
		case resp, ok := <-rawCh:
			if !ok {
				return true, nil
			}

			*revision = resp.Header.Revision

			if err := resp.Err(); err != nil {
				if !created {
					// Stop on initialization error
					outCh <- Events{InitErr: err}
					return true, err
				}
				handleErr(err)
				continue
			}

			if resp.Created {
				created = true
			}

			// Sort events from the batch (if multiple keys have been modified in one txn, in one revision)
			// 1. By type, PUT before DELETE
			// 2. By key, A->Z
			sort.SliceStable(resp.Events, func(i, j int) bool {
				if resp.Events[i].Type != resp.Events[j].Type {
					return resp.Events[i].Type < resp.Events[j].Type
				}
				return string(resp.Events[i].Kv.Key) < string(resp.Events[j].Kv.Key)
			})

			outEvents := make([]Event, len(resp.Events))
			for i, rawEvent := range resp.Events {
				outEvent := Event{Kv: rawEvent.Kv, PrevKv: rawEvent.PrevKv}

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

				outEvents[i] = outEvent
			}

			outCh <- Events{
				Header:  &resp.Header,
				Events:  outEvents,
				Created: resp.Created,
			}
		}
	}
}
