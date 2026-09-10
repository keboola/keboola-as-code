package sessions

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// drainTimeout bounds how long close waits for queued events when the shutdown
// context carries no deadline of its own.
//
// Sized against the pod's total grace period, not on its own: shutdown
// callbacks run sequentially, the HTTP server drains first for up to
// gracefulShutdownTimeout, and the pod is SIGKILLed 30 s after SIGTERM. 20 s
// there plus 5 s here leaves headroom; raising either means checking the sum.
const drainTimeout = 5 * time.Second

// One POST per record, so events are queued and sent by a worker pool.
//
// Retries are deliberately absent: a retried heartbeat would land as a second
// row carrying the same delta. That is also why this uses a plain http.Client
// rather than the repo's instrumented one, which retries.
type writer struct {
	logger    log.Logger
	metrics   *metrics
	url       string
	client    *http.Client
	queue     chan Event
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	dropped   atomic.Uint64
	failed    atomic.Uint64
}

type writerConfig struct {
	url         string
	queueSize   int
	workers     int
	sendTimeout time.Duration
}

func newWriter(logger log.Logger, m *metrics, cfg writerConfig) *writer {
	// http.DefaultTransport keeps two idle connections per host, so with more
	// workers the rest would pay a TLS handshake per send, charged against
	// sendTimeout. Cloned rather than built, to keep its proxy and dial setup.
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert
	transport.MaxIdleConnsPerHost = cfg.workers
	transport.MaxIdleConns = cfg.workers * 2

	w := &writer{
		logger:  logger,
		metrics: m,
		url:     cfg.url,
		client:  &http.Client{Timeout: cfg.sendTimeout, Transport: transport},
		queue:   make(chan Event, cfg.queueSize),
		done:    make(chan struct{}),
	}

	for range cfg.workers {
		w.wg.Add(1)
		go w.run()
	}

	return w
}

// enqueue hands the event to a worker. It never blocks: session tracking must
// not add latency to a user's request, and must not stall when Stream is slow
// or unreachable. A full queue drops the event and bumps a counter.
//
// The queue is never closed — a websocket can close after shutdown has begun
// (http.Server.Shutdown does not wait for hijacked connections), and a send on
// a closed channel panics even inside a select with a default branch.
func (w *writer) enqueue(ctx context.Context, event Event) {
	select {
	case w.queue <- event:
	default:
		w.metrics.dropped.Add(ctx, 1)
		if n := w.dropped.Add(1); n == 1 || n%1000 == 0 {
			w.logger.Warnf(ctx, "session event queue is full, dropped %d events so far", n)
		}
	}
}

func (w *writer) run() {
	defer w.wg.Done()
	for {
		select {
		case event := <-w.queue:
			w.send(event)
		case <-w.done:
			w.drain()
			return
		}
	}
}

// drain sends what is already buffered and gives up as soon as the queue runs
// dry, so a shutdown is not held up by events that arrive afterwards.
func (w *writer) drain() {
	for {
		select {
		case event := <-w.queue:
			w.send(event)
		default:
			return
		}
	}
}

func (w *writer) send(event Event) {
	// Detached from any request context: the request that produced the event
	// may already be finished, and its cancellation must not kill the send.
	ctx, cancel := context.WithTimeoutCause(context.Background(), w.client.Timeout, errors.New("session event send timeout"))
	defer cancel()

	body, err := json.Marshal(event)
	if err != nil {
		w.logger.Errorf(ctx, "cannot serialize session event: %s", err.Error())
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		// Rate-limited and sanitized like any other send failure: a malformed
		// stream url fails on every single event, and the error carries the url.
		w.recordFailure(ctx, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		w.recordFailure(ctx, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain the body so the connection can be reused for the next event.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= http.StatusBadRequest {
		w.recordFailure(ctx, errors.Errorf("unexpected status %s", resp.Status))
		return
	}

	w.metrics.sent.Add(ctx, 1, sentAttrs(nil))
}

// recordFailure logs sparsely on purpose: when Stream is down every event
// fails, and logging each one would bury everything else.
func (w *writer) recordFailure(ctx context.Context, err error) {
	w.metrics.sent.Add(ctx, 1, sentAttrs(err))
	if n := w.failed.Add(1); n == 1 || n%1000 == 0 {
		w.logger.Warnf(ctx, "cannot send session event (%d failed so far): %s", n, sanitize(err))
	}
}

// sanitize strips the request URL out of an error before it is logged.
//
// net/http wraps transport failures in *url.Error, whose Error() embeds the
// full URL — which here is the Stream ingest URL including its write secret.
// The config marks that URL sensitive so it never appears in a config dump;
// logging it from an error path on every Stream outage would defeat that.
func sanitize(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Op + ": " + urlErr.Err.Error()
	}
	return err.Error()
}

// close stops the workers and waits for the already-queued events, bounded by
// ctx (or drainTimeout when it has no deadline). Without a bound a full queue
// against an unreachable Stream would hold the shutdown for queueSize ×
// sendTimeout ÷ workers — long enough for Kubernetes to SIGKILL the pod and
// turn a graceful shutdown into a hard one.
func (w *writer) close(ctx context.Context) {
	w.closeOnce.Do(func() { close(w.done) })

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, drainTimeout, errors.New("session event drain timeout"))
		defer cancel()
	}

	finished := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-ctx.Done():
		w.logger.Warnf(ctx, "gave up draining session events, %d still queued", len(w.queue))
	}
}
