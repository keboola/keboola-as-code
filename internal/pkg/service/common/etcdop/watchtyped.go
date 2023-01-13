package etcdop

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

type WatchEventT[T any] struct {
	Type   EventType
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
	Value  T
}

type WatchResponseT[T any] struct {
	WatcherStatus
	Events []WatchEventT[T]
}

func (e *WatchResponseT[T]) Rev() int64 {
	return e.Header.Revision
}

// GetAllAndWatch loads all keys in the prefix by the iterator and then watch for changes.
// Values are decoded to the type T.
//
// If a fatal error occurs, the watcher is restarted.
// The "restarted" event is emitted before the restart.
// Then, the following events are streamed from the beginning.
//
// See WatchResponse for details.
func (v PrefixT[T]) GetAllAndWatch(ctx context.Context, client *etcd.Client, opts ...etcd.OpOption) (out <-chan WatchResponseT[T]) {
	return v.decodeChannel(ctx, func(ctx context.Context) <-chan WatchResponse {
		return v.prefix.GetAllAndWatch(ctx, client, opts...)
	})
}

// Watch method wraps low-level etcd watcher.
// Values are decoded to the type T.
//
// In addition, if a fatal error occurs, the watcher is restarted.
// The "restarted" event is emitted before the restart.
// Then, the following events are streamed from the beginning.
//
// If the InitErr occurs during the first attempt to create the watcher,
// the operation is stopped and the restart is not performed.
//
// See WatchResponse for details.
func (v PrefixT[T]) Watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) <-chan WatchResponseT[T] {
	return v.decodeChannel(ctx, func(ctx context.Context) <-chan WatchResponse {
		return v.prefix.Watch(ctx, client, opts...)
	})
}

// decodeChannel is used by Watch and GetAllAndWatch to decode raw data to typed data.
func (v PrefixT[T]) decodeChannel(ctx context.Context, channelFactory func(ctx context.Context) <-chan WatchResponse) <-chan WatchResponseT[T] {
	outCh := make(chan WatchResponseT[T])
	go func() {
		defer close(outCh)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Decode value, if an error occurs, send it through the channel.
		decode := func(kv *op.KeyValue, header *etcdserverpb.ResponseHeader) (T, bool) {
			var target T
			if err := v.serde.Decode(ctx, kv, &target); err != nil {
				resp := WatchResponseT[T]{}
				resp.Header = header
				resp.Err = err
				outCh <- resp
				return target, false
			}
			return target, true
		}

		// Channel is closed by the context, so the context does not have to be checked here again.
		rawCh := channelFactory(ctx)
		for rawResp := range rawCh {
			var events []WatchEventT[T]
			if len(rawResp.Events) > 0 {
				events = make([]WatchEventT[T], 0, len(rawResp.Events))
			}

			// Map raw response to typed response.
			for _, rawEvent := range rawResp.Events {
				// Decode value.
				var value T
				var ok bool
				if rawEvent.Type == CreateEvent || rawEvent.Type == UpdateEvent {
					// Always decode create/update value.
					value, ok = decode(rawEvent.Kv, rawResp.Header)
					if !ok {
						continue
					}
				} else if rawEvent.Type == DeleteEvent && rawEvent.PrevKv != nil {
					// Decode previous value on delete, if is present.
					// etcd.WithPrevKV() option must be used to enable it.
					value, ok = decode(rawEvent.PrevKv, rawResp.Header)
					if !ok {
						continue
					}
				}

				events = append(events, WatchEventT[T]{
					Type:   rawEvent.Type,
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
					Value:  value,
				})
			}

			// Pass the response
			outCh <- WatchResponseT[T]{
				WatcherStatus: rawResp.WatcherStatus,
				Events:        events,
			}
		}
	}()

	return outCh
}
