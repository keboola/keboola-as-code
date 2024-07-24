package etcdop

import (
	"bytes"
	"context"
	"sort"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// getAllBatchSize defines batch size for "getAll" phase of the GetAllAndWatch operation.
	getAllBatchSize = 100
	// watchErrorThreshold - if less than the interval elapses between two watch errors, it is not a warning, but an error.
	watchErrorThreshold = 5 * time.Second
)

type WatcherStatus struct {
	Header *Header
	// InitErr is used to indicate an error during initialization of the watcher, before the first event is received.
	InitErr error
	// Err is used to indicate an error.
	// Fatal error is followed by the "Restarted" event.
	Err error
	// Created is used to indicate the creation of the watcher, it is emitted before the first event.
	Created bool
	// Restarted is used to indicate re-creation of the watcher.
	// Prefix.GetAllAndWatch and PrefixT.GetAllAndWatch watchers will stream all records again.
	// Restart is triggered in case of a fatal error (such as ErrCompacted) from which it is not possible to recover.
	// Restart can be triggerred also manually by RestartableWatchStreamRaw.Restart method.
	Restarted    bool
	RestartCause error
	RestartDelay time.Duration
}

// WatchStreamE streams events of the E type.
type WatchStreamE[E any] struct {
	channel     chan WatchResponseE[E]
	cancel      context.CancelCauseFunc
	cancelCause error
}

// WatchStreamRaw for untyped prefix.
type WatchStreamRaw = WatchStreamE[WatchEvent[[]byte]]

func (s *WatchStreamE[E]) Channel() <-chan WatchResponseE[E] {
	return s.channel
}

// GetAllAndWatch loads all keys in the prefix by the iterator and then Watch for changes.
//   - Connection of GetAll and Watch phase is atomic, the etcd.WithRev option is used.
//   - If an error occurs during initialization, the operation is halted, it is signalized by the WatcherStatus.InitErr field.
//   - After successful initialization, the WatcherStatus.Created = true event is emitted.
//   - Recoverable errors are automatically retried in the background by the low-level etcd client.
//   - If a fatal error occurs after initialization (such as ErrCompacted), the watcher is automatically restarted.
//   - The retry mechanism uses exponential backoff for subsequent attempts.
//   - When a restart occurs, the WatcherStatus.Restarted = true is emitted.
//   - Then, the following events are streamed from the beginning.
//   - Restart can be triggered also manually by the RestartableWatchStreamRaw.Restart method.
//   - The operation can be cancelled using the context.
func (v Prefix) GetAllAndWatch(ctx context.Context, client *etcd.Client, opts ...etcd.OpOption) *RestartableWatchStreamRaw {
	return wrapStreamWithRestart(ctx, func(ctx context.Context) *WatchStreamRaw {
		ctx, cancel := context.WithCancelCause(ctx)
		stream := &WatchStreamRaw{channel: make(chan WatchResponseRaw), cancel: cancel}
		go func() {
			defer close(stream.channel)
			defer cancel(context.Canceled)

			// GetAll phase
			itr := v.GetAll(client).Do(ctx)
			var events []WatchEvent[[]byte]
			sendBatch := func() {
				if len(events) > 0 {
					resp := WatchResponseRaw{}
					resp.Header = itr.Header()
					resp.Events = events
					events = nil
					stream.channel <- resp
				}
			}

			// Iterate and send batches of events
			i := 1
			err := itr.ForEach(func(kv *op.KeyValue, _ *iterator.Header) error {
				events = append(events, WatchEvent[[]byte]{Kv: kv, Type: CreateEvent, Key: string(kv.Key), Value: kv.Value})
				if i%getAllBatchSize == 0 {
					sendBatch()
				}
				return nil
			})
			sendBatch()

			// Process GetAll error
			if err != nil {
				resp := WatchResponseRaw{}
				resp.InitErr = err
				stream.channel <- resp

				// Stop
				return
			}

			// Watch phase, continue  where the GetAll operation ended (revision + 1)
			rawStream := v.WatchWithoutRestart(ctx, client, append([]etcd.OpOption{etcd.WithRev(itr.Header().Revision + 1)}, opts...)...)
			for resp := range rawStream.channel {
				stream.channel <- resp
			}
		}()

		return stream
	})
}

// Watch method wraps the low-level etcd watcher to watch for changes in the prefix.
//   - If an error occurs during initialization, the operation is halted, and it is signalized by the event.InitErr field.
//   - After successful initialization, the WatcherStatus.Created = true event is emitted.
//   - Recoverable errors are automatically retried in the background by the low-level etcd client.
//   - If a fatal error occurs after initialization (such as etcd ErrCompacted), the watcher is automatically restarted.
//   - The retry mechanism uses exponential backoff for subsequent attempts.
//   - When a restart occurs, the WatcherStatus.Restarted = true is emitted.
//   - Restart can be triggered also manually by the RestartableWatchStreamRaw.Restart method.
//   - The operation can be cancelled using the context.
func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) *RestartableWatchStreamRaw {
	return wrapStreamWithRestart(ctx, func(ctx context.Context) *WatchStreamRaw {
		return v.WatchWithoutRestart(ctx, client, opts...)
	})
}

// WatchWithoutRestart is same as the Watch, but watcher is not restarted on a fatal error.
func (v Prefix) WatchWithoutRestart(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) *WatchStreamRaw {
	ctx, cancel := context.WithCancelCause(ctx)
	stream := &WatchStreamRaw{channel: make(chan WatchResponseRaw), cancel: cancel}
	go func() {
		defer close(stream.channel)
		defer cancel(context.Canceled)

		// The initialization phase lasts until etcd sends the "created" event.
		// It is the first event that is sent.
		// The application logic usually waits for this event when the application starts.
		// At most one WatchResponseRaw.InitErr will be emitted.
		init := true

		// End fast, if the context is cancelled
		if ctx.Err() != nil {
			return
		}

		// The rawCh channel is closed by the context, so the context does not have to be checked here again.
		rawCh := client.Watch(ctx, v.Prefix(), append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCreatedNotify()}, opts...)...)
		for rawResp := range rawCh {
			header := rawResp.Header
			resp := WatchResponseRaw{}
			resp.Header = &header
			resp.Created = rawResp.Created

			// Handle error
			if err := rawResp.Err(); err != nil {
				if init {
					// Pass initialization error
					resp.InitErr = errors.Errorf(`watch init error: %w`, err)
					stream.channel <- resp

					// Stop watching
					return
				} else {
					// Pass other error
					resp.Err = errors.Errorf(`watch error: %w`, err)
					stream.channel <- resp

					// If the error is fatal, then the rawCh will be closed in the next iteration.
					// Otherwise, continue.
					continue
				}
			}

			// Stop initialization phase after the "created" event
			if rawResp.Created {
				init = false
				stream.channel <- resp
				continue
			}

			// Sort events from the batch (if multiple keys have been modified in one txn, in one revision)
			// 1. By type, PUT before DELETE
			// 2. By key, A->Z
			sort.SliceStable(rawResp.Events, func(i, j int) bool {
				if rawResp.Events[i].Type != rawResp.Events[j].Type {
					return rawResp.Events[i].Type < rawResp.Events[j].Type
				}
				return bytes.Compare(rawResp.Events[i].Kv.Key, rawResp.Events[j].Kv.Key) == -1
			})

			if len(rawResp.Events) > 0 {
				resp.Events = make([]WatchEvent[[]byte], 0, len(rawResp.Events))
			}

			// Map event type
			for _, rawEvent := range rawResp.Events {
				var typ EventType
				switch {
				case rawEvent.IsCreate():
					typ = CreateEvent
				case rawEvent.IsModify():
					typ = UpdateEvent
				case rawEvent.Type == mvccpb.DELETE:
					typ = DeleteEvent
				default:
					panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
				}

				out := WatchEvent[[]byte]{
					Type:   typ,
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
					Key:    string(rawEvent.Kv.Key),
					Value:  rawEvent.Kv.Value,
				}

				if out.PrevKv != nil {
					out.PrevValue = &out.PrevKv.Value
				}

				resp.Events = append(resp.Events, out)
			}

			// Pass the response
			stream.channel <- resp
		}

		// Send init error, if the context has been cancelled before the "Created" event
		if err := ctx.Err(); err != nil && init {
			resp := WatchResponseRaw{}
			resp.InitErr = errors.Errorf(`watch cancelled: %w`, err)
			stream.channel <- resp
		}
	}()

	return stream
}
