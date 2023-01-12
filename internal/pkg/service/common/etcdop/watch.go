package etcdop

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
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

const (
	// getAllBatchSize defines batch size for "getAll" phase of the GetAllAndWatch operation.
	getAllBatchSize = 100
)

type WatchEvent struct {
	Type   EventType
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
}

type WatcherStatus struct {
	Header *etcdserverpb.ResponseHeader
	// InitErr is used to indicate an error during initialization of the watcher, before the first event is received.
	InitErr error
	// Err is used to indicate an error.
	// Fatal error is followed by the "Restarted" event.
	Err error
	// Created is used to indicate the creation of the watcher, it is emitted before the first event.
	Created bool
	// Restarted is used to indicate re-creation of the watcher, the following events are streamed from the beginning.
	// It is used in case of a fatal error (etcd ErrCompacted) from which it is not possible to recover.
	Restarted     bool
	RestartReason error
	RestartDelay  time.Duration
}

type WatchResponse struct {
	WatcherStatus
	Events []WatchEvent
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

func (e *WatchResponse) Rev() int64 {
	return e.Header.Revision
}

// Watch method wraps low-level etcd watcher.
// In addition, if a fatal error occurs, the watcher is restarted.
// The "restarted" event is emitted before the restart.
// Then, the following events are streamed from the beginning.
//
// If the InitErr occurs during the first attempt to create the watcher,
// the operation is stopped and the restart is not performed.
//
// See WatchResponse for details.
func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) <-chan WatchResponse {
	return wrapWatchWithRestart(ctx, func(ctx context.Context) <-chan WatchResponse {
		return v.watch(ctx, client, opts...)
	})
}

// GetAllAndWatch loads all keys in the prefix by the iterator and then watch for changes.
//
// If a fatal error occurs, the watcher is restarted.
// The "restarted" event is emitted before the restart.
// Then, the following events are streamed from the beginning.
//
// See WatchResponse for details.
func (v Prefix) GetAllAndWatch(ctx context.Context, client *etcd.Client, opts ...etcd.OpOption) (out <-chan WatchResponse) {
	return wrapWatchWithRestart(ctx, func(ctx context.Context) <-chan WatchResponse {
		outCh := make(chan WatchResponse)
		go func() {
			defer close(outCh)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// GetAll phase
			itr := v.GetAll().Do(ctx, client)
			var events []WatchEvent
			sendBatch := func() {
				if len(events) > 0 {
					resp := WatchResponse{}
					resp.Header = itr.Header()
					resp.Events = events
					events = nil
					outCh <- resp
				}
			}

			// Iterate and send batches of events
			i := 1
			err := itr.ForEach(func(kv *op.KeyValue, _ *etcdserverpb.ResponseHeader) error {
				events = append(events, WatchEvent{Kv: kv, Type: CreateEvent})
				if i%getAllBatchSize == 0 {
					sendBatch()
				}
				return nil
			})
			sendBatch()

			// Process GetAll error
			if err != nil {
				resp := WatchResponse{}
				resp.InitErr = err
				events = nil
				outCh <- resp

				// Stop
				return
			}

			// Watch phase, continue  where the GetAll operation ended (revision + 1)
			rawCh := v.watch(ctx, client, append([]etcd.OpOption{etcd.WithRev(itr.Header().Revision + 1)}, opts...)...)
			for resp := range rawCh {
				outCh <- resp
			}
		}()

		return outCh
	})
}

// watch the Prefix, operation can be cancelled by the context or a fatal error (etcd ErrCompacted).
// Otherwise, watch will retry on other recoverable errors forever until reconnected.
func (v Prefix) watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) <-chan WatchResponse {
	outCh := make(chan WatchResponse)
	go func() {
		defer close(outCh)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// The initialization phase lasts until etcd sends the "created" event.
		// It is the first event that is sent.
		// The application logic usually waits for this event when the application starts.
		// At most one WatchResponse.InitErr will be emitted.
		init := true

		// The rawCh channel is closed by the context, so the context does not have to be checked here again.
		rawCh := client.Watch(ctx, v.Prefix(), append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCreatedNotify()}, opts...)...)
		for rawResp := range rawCh {
			resp := WatchResponse{}
			resp.Header = &rawResp.Header
			resp.Created = rawResp.Created

			// Handle error
			if err := rawResp.Err(); err != nil {
				if init {
					// Pass initialization error
					resp.InitErr = err
					outCh <- resp

					// Stop
					return
				} else {
					// Pass other error
					resp.Err = err
					outCh <- resp
					continue
				}
			}

			// Stop initialization phase after the "created" event
			if rawResp.Created {
				init = false
				outCh <- resp
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
				resp.Events = make([]WatchEvent, 0, len(rawResp.Events))
			}

			// Map event type
			for _, rawEvent := range rawResp.Events {
				var typ EventType
				switch rawEvent.Type {
				case mvccpb.PUT:
					if rawEvent.Kv.CreateRevision == rawEvent.Kv.ModRevision {
						typ = CreateEvent
					} else {
						typ = UpdateEvent
					}
				case mvccpb.DELETE:
					typ = DeleteEvent
				default:
					panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
				}

				resp.Events = append(resp.Events, WatchEvent{
					Type:   typ,
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
				})
			}

			// Pass the response
			outCh <- resp
		}
	}()

	return outCh
}

func wrapWatchWithRestart(ctx context.Context, chanFactory func(ctx context.Context) <-chan WatchResponse) <-chan WatchResponse {
	b := backoff.WithContext(newWatchBackoff(), ctx)
	outCh := make(chan WatchResponse)
	go func() {
		defer close(outCh)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// The initialization phase lasts until the first "created" event.
		// If the watch operation was restarted,
		// the next initialization error is converted to a common error.
		init := true

		// The "restarted" event contains RestartReason - last error.
		var lastErr error

		for {
			// The rawCh channel is closed by the context, so the context does not have to be checked here again.
			rawCh := chanFactory(ctx)
			for resp := range rawCh {
				// Stop initialization phase after the "created" event
				if resp.Created {
					init = false
				}

				// Update lastErr for "restarted" event
				if resp.InitErr != nil {
					lastErr = resp.InitErr
				} else if resp.Err != nil {
					lastErr = resp.Err
				}

				// Handle initialization error
				if resp.InitErr != nil {
					if init {
						// Stop on initialization error
						outCh <- resp
						return
					} else {
						// Convert initialization error
						// from an 1+ attempt to a common error and restart watch.
						resp.Err = resp.InitErr
						resp.InitErr = nil
						outCh <- resp
						break
					}
				}

				// Pass the response
				outCh <- resp
			}

			// Underlying watcher has stopped, restart
			delay := b.NextBackOff()
			if delay == backoff.Stop {
				return
			}

			// Wait before restart
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
				// continue
			}

			// Emit "restarted" event
			resp := WatchResponse{}
			resp.Restarted = true
			resp.RestartReason = errors.Errorf(`restarted after delay %s, reason: %s`, delay, lastErr)
			resp.RestartDelay = delay
			outCh <- resp
		}
	}()

	return outCh
}

func newWatchBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 1 * time.Minute
	b.MaxElapsedTime = 0 // never stop
	b.Reset()
	return b
}
