package etcdop

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
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
	// watchErrorThreshold - if less than the interval elapses between two watch errors, it is not a warning, but an error.
	watchErrorThreshold = 5 * time.Second
)

type WatchEvent struct {
	Type   EventType
	Kv     *op.KeyValue
	PrevKv *op.KeyValue
}

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
	// Restart can be triggerred also manually by RestartableWatchStream.Restart method.
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

// RestartableWatchStream is restarted on a fatal error, or manually by the Restart method.
type RestartableWatchStream struct {
	WatchStreamE[WatchEvent]
	lock *sync.Mutex
	sub  *WatchStreamE[WatchEvent]
}

// WatchStream for untyped prefix.
type WatchStream = WatchStreamE[WatchEvent]

// WatchResponse for untyped prefix.
type WatchResponse = WatchResponseE[WatchEvent]

// WatchResponseE wraps events of the type E together with WatcherStatus.
type WatchResponseE[E any] struct {
	WatcherStatus
	Events []E
}

func (e *WatchResponseE[E]) Rev() int64 {
	return e.Header.Revision
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

func (s *WatchStreamE[E]) Channel() <-chan WatchResponseE[E] {
	return s.channel
}

func (s *WatchStreamE[E]) SetupConsumer(logger log.Logger) WatchConsumer[E] {
	return newConsumer[E](logger, s)
}

// Restart cancels the current stream, so a new stream is created.
func (s RestartableWatchStream) Restart(cause error) {
	s.lock.Lock()
	if s.sub != nil {
		s.sub.cancelCause = cause
		s.sub.cancel(cause)
		for range s.sub.channel {
			// wait for the channel close
		}
	}
	s.lock.Unlock()
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
//   - Restart can be triggered also manually by the RestartableWatchStream.Restart method.
//   - The operation can be cancelled using the context.
func (v Prefix) GetAllAndWatch(ctx context.Context, client *etcd.Client, opts ...etcd.OpOption) *RestartableWatchStream {
	return wrapStreamWithRestart(ctx, func(ctx context.Context) *WatchStream {
		ctx, cancel := context.WithCancelCause(ctx)
		stream := &WatchStream{channel: make(chan WatchResponse), cancel: cancel}
		go func() {
			defer close(stream.channel)
			defer cancel(context.Canceled)

			// GetAll phase
			itr := v.GetAll(client).Do(ctx)
			var events []WatchEvent
			sendBatch := func() {
				if len(events) > 0 {
					resp := WatchResponse{}
					resp.Header = itr.Header()
					resp.Events = events
					events = nil
					stream.channel <- resp
				}
			}

			// Iterate and send batches of events
			i := 1
			err := itr.ForEach(func(kv *op.KeyValue, _ *Header) error {
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
//   - Restart can be triggered also manually by the RestartableWatchStream.Restart method.
//   - The operation can be cancelled using the context.
func (v Prefix) Watch(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) *RestartableWatchStream {
	return wrapStreamWithRestart(ctx, func(ctx context.Context) *WatchStream {
		return v.WatchWithoutRestart(ctx, client, opts...)
	})
}

// WatchWithoutRestart is same as the Watch, but watcher is not restarted on a fatal error.
func (v Prefix) WatchWithoutRestart(ctx context.Context, client etcd.Watcher, opts ...etcd.OpOption) *WatchStream {
	ctx, cancel := context.WithCancelCause(ctx)
	stream := &WatchStream{channel: make(chan WatchResponse), cancel: cancel}
	go func() {
		defer close(stream.channel)
		defer cancel(context.Canceled)

		// The initialization phase lasts until etcd sends the "created" event.
		// It is the first event that is sent.
		// The application logic usually waits for this event when the application starts.
		// At most one WatchResponse.InitErr will be emitted.
		init := true

		// The rawCh channel is closed by the context, so the context does not have to be checked here again.
		rawCh := client.Watch(ctx, v.Prefix(), append([]etcd.OpOption{etcd.WithPrefix(), etcd.WithCreatedNotify()}, opts...)...)
		for rawResp := range rawCh {
			header := rawResp.Header
			resp := WatchResponse{}
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
				resp.Events = make([]WatchEvent, 0, len(rawResp.Events))
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

				resp.Events = append(resp.Events, WatchEvent{
					Type:   typ,
					Kv:     rawEvent.Kv,
					PrevKv: rawEvent.PrevKv,
				})
			}

			// Pass the response
			stream.channel <- resp
		}

		// Send init error, if the context has been cancelled before the "Created" event
		if err := ctx.Err(); err != nil && init {
			resp := WatchResponse{}
			resp.InitErr = errors.Errorf(`watch cancelled: %w`, err)
			stream.channel <- resp
		}
	}()

	return stream
}

// wrapStreamWithRestart continuously tries to restart the stream on fatal errors.
// An exponential backoff is used between attempts.
// The operation can be cancelled using the context.
func wrapStreamWithRestart(ctx context.Context, channelFactory func(ctx context.Context) *WatchStream) *RestartableWatchStream {
	b := backoff.WithContext(newWatchBackoff(), ctx)
	ctx, cancel := context.WithCancelCause(ctx)
	stream := &RestartableWatchStream{WatchStreamE: WatchStream{channel: make(chan WatchResponse), cancel: cancel}, lock: &sync.Mutex{}}
	go func() {
		defer close(stream.channel)
		defer cancel(context.Canceled)

		// The initialization phase lasts until the first "created" event.
		// If the watch operation was restarted,
		// the next initialization error is converted to a common error.
		init := true

		// The "restarted" event contains RestartCause - last error.
		var lastErr error
		var restart bool
		var restartCause error
		var restartDelay time.Duration

		for {
			// Is context done?
			if ctx.Err() != nil {
				return
			}

			// Store reference to the current stream, so it can be canceled/restarted
			subStream := channelFactory(ctx)
			stream.lock.Lock()
			stream.sub = subStream
			stream.lock.Unlock()

			for resp := range subStream.channel {
				// Emit "restarted" event before the first event after the restart
				if restart {
					var cause error
					switch {
					case lastErr != nil:
						cause = errors.PrefixErrorf(lastErr, `unexpected restart, backoff delay %s, cause:`, restartDelay)
					case restartCause != nil:
						cause = restartCause
					default:
						cause = errors.New("unknown cause") // shouldn't happen
					}

					rst := WatchResponse{}
					rst.Restarted = true
					rst.RestartCause = cause
					rst.RestartDelay = restartDelay
					stream.channel <- rst
					lastErr = nil
					restart = false
					restartCause = nil
				}

				// Pass OnCreated to the stream.channel channel
				if resp.Created {
					init = false
					b.Reset()
				}

				// Update lastErr for RestartCause
				if resp.InitErr != nil {
					lastErr = resp.InitErr
				} else if resp.Err != nil {
					lastErr = resp.Err
				}

				// Handle initialization error
				if err := resp.InitErr; err != nil {
					if init { // nolint: gocritic
						// Stop on initialization error
						stream.channel <- resp
						return
					} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						// Context cancelled event is forwarded only during the initialization.
						// In other cases, closing the channel is sufficient.
						continue
					} else {
						// Convert initialization error
						// from an 1+ attempt to a common error and restart watch.
						resp.Err = err
						resp.InitErr = nil
						stream.channel <- resp
						break
					}
				}

				// Pass the response
				stream.channel <- resp
			}

			// Restart is in progress
			restart = true

			// Delay is applied only if the restart is caused by an error, not by the manual restart
			var delay time.Duration
			if lastErr != nil {
				// Calculate delay
				restartDelay = b.NextBackOff()
				if restartDelay == backoff.Stop {
					return
				}

				// Wait before restart
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
					// continue
				}
			} else if subStream.cancelCause != nil {
				// If the stream was cancelled manually (without error), save the cancellation reason
				restartCause = subStream.cancelCause
			}
		}
	}()

	return stream
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
