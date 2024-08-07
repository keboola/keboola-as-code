package etcdop

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

// WatchStreamT streams events of the WatchEvent[T] type.
type WatchStreamT[T any] WatchStreamE[WatchEvent[T]]

func (s *WatchStreamT[T]) WatchedPrefix() string {
	return s.prefix
}

func (s *WatchStreamT[T]) Channel() <-chan WatchResponseE[WatchEvent[T]] {
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
func (v PrefixT[T]) GetAllAndWatch(ctx context.Context, client *etcd.Client, opts ...etcd.OpOption) *RestartableWatchStreamT[T] {
	rawStream := v.prefix.GetAllAndWatch(ctx, client, opts...)
	decodedStream := v.decodeChannel(ctx, &rawStream.WatchStreamE)
	return &RestartableWatchStreamT[T]{
		WatchStreamT: decodedStream,
		rawStream:    rawStream,
	}
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
func (v PrefixT[T]) Watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) *RestartableWatchStreamT[T] {
	rawStream := v.prefix.Watch(ctx, client, opts...)
	decodedStream := v.decodeChannel(ctx, &rawStream.WatchStreamE)
	return &RestartableWatchStreamT[T]{
		WatchStreamT: decodedStream,
		rawStream:    rawStream,
	}
}

// WatchWithoutRestart is same as the Watch, but watcher is not restarted on a fatal error.
func (v PrefixT[T]) WatchWithoutRestart(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) *WatchStreamT[T] {
	rawStream := v.prefix.WatchWithoutRestart(ctx, client, opts...)
	return v.decodeChannel(ctx, rawStream)
}

// decodeChannel is used by Watch and GetAllAndWatch to decode raw data to typed data.
func (v PrefixT[T]) decodeChannel(ctx context.Context, rawStream *WatchStreamRaw) *WatchStreamT[T] {
	ctx, cancel := context.WithCancelCause(ctx)
	stream := &WatchStreamT[T]{prefix: v.Prefix(), channel: make(chan WatchResponseE[WatchEvent[T]]), cancel: cancel}
	go func() {
		defer close(stream.channel)
		defer cancel(context.Canceled)

		// Decode value, if an error occurs, send it through the channel.
		decode := func(kv *op.KeyValue, header *Header) (T, bool) {
			var target T
			if err := v.serde.Decode(ctx, kv, &target); err != nil {
				resp := WatchResponseE[WatchEvent[T]]{}
				resp.Header = header
				resp.Err = err
				stream.channel <- resp
				return target, false
			}
			return target, true
		}

		// Channel is closed by the context, so the context does not have to be checked here again.
		for rawResp := range rawStream.channel {
			var events []WatchEvent[T]
			if len(rawResp.Events) > 0 {
				events = make([]WatchEvent[T], 0, len(rawResp.Events))
			}

			// Map raw response to typed response.
			for _, rawEvent := range rawResp.Events {
				outEvent := WatchEvent[T]{
					Type:   rawEvent.Type,
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
					Key:    rawEvent.Key,
				}

				// Decode value.
				var ok bool
				if rawEvent.Type == CreateEvent || rawEvent.Type == UpdateEvent {
					// Always decode create/update value.
					if outEvent.Value, ok = decode(rawEvent.Kv, rawResp.Header); !ok {
						continue
					}
				} else if rawEvent.Type == DeleteEvent && rawEvent.PrevKv != nil {
					// Decode previous value on delete, if is present.
					// etcd.WithPrevKV() option must be used to enable it.
					if outEvent.Value, ok = decode(rawEvent.PrevKv, rawResp.Header); !ok {
						continue
					}
				}

				// Decode previous value on update, if it is present.
				// etcd.WithPrevKV() option must be used to enable it.
				if rawEvent.Type == UpdateEvent && rawEvent.PrevKv != nil {
					if prevValue, ok := decode(rawEvent.PrevKv, rawResp.Header); ok {
						outEvent.PrevValue = &prevValue
					} else {
						continue
					}
				}

				events = append(events, outEvent)
			}

			// Pass the response
			stream.channel <- WatchResponseE[WatchEvent[T]]{
				WatcherStatus: rawResp.WatcherStatus,
				Events:        events,
			}
		}
	}()

	return stream
}
