package sessions

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func testWriter(t *testing.T, url string, queueSize, workers int) *writer {
	t.Helper()
	return newWriter(log.NewNopLogger(), writerConfig{
		url:         url,
		queueSize:   queueSize,
		workers:     workers,
		sendTimeout: 2 * time.Second,
	})
}

func TestWriter_Send(t *testing.T) {
	t.Parallel()

	var mutex sync.Mutex
	var got []Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var event Event
		if !assert.NoError(t, json.NewDecoder(req.Body).Decode(&event)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mutex.Lock()
		got = append(got, event)
		mutex.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	w := testWriter(t, server.URL, 16, 2)
	w.enqueue(t.Context(), Event{EventType: EventSessionStart, SessionID: "a"})
	w.enqueue(t.Context(), Event{EventType: EventHeartbeat, SessionID: "a"})

	// close drains what is queued, so no polling is needed.
	w.close(t.Context())

	mutex.Lock()
	defer mutex.Unlock()
	assert.Len(t, got, 2)
	assert.Zero(t, w.dropped.Load())
	assert.Zero(t, w.failed.Load())
}

func TestWriter_DropsWhenQueueIsFull(t *testing.T) {
	t.Parallel()

	// No workers, so nothing is ever taken off the queue.
	w := testWriter(t, "http://127.0.0.1:1/", 1, 0)

	for range 5 {
		w.enqueue(t.Context(), Event{SessionID: "a"})
	}

	// Dropping rather than blocking is deliberate: session tracking must not
	// add latency to a user's request when Stream is slow or unreachable.
	assert.Equal(t, uint64(4), w.dropped.Load())
}

func TestWriter_EnqueueAfterCloseDoesNotPanic(t *testing.T) {
	t.Parallel()

	w := testWriter(t, "http://127.0.0.1:1/", 4, 1)
	w.close(t.Context())

	// Reachable in production: http.Server.Shutdown does not wait for hijacked
	// connections, so a websocket can close — and end its session — after the
	// writer has been shut down. A send on a closed channel would panic even
	// inside a select with a default branch, which is why the queue is never
	// closed.
	assert.NotPanics(t, func() {
		w.enqueue(context.WithoutCancel(t.Context()), Event{EventType: EventSessionEnd, SessionID: "a"})
	})
}

func TestWriter_CloseIsIdempotent(t *testing.T) {
	t.Parallel()

	w := testWriter(t, "http://127.0.0.1:1/", 4, 1)
	assert.NotPanics(t, func() {
		w.close(t.Context())
		w.close(t.Context())
	})
}

func TestWriter_DoesNotLogTheStreamSecret(t *testing.T) {
	t.Parallel()

	const secret = "THIS-IS-THE-WRITE-SECRET"

	logger := log.NewDebugLogger()
	w := newWriter(logger, writerConfig{
		// Unroutable, so the send fails and the failure gets logged.
		url:         "http://127.0.0.1:1/stream/123/sessions/" + secret,
		queueSize:   4,
		workers:     1,
		sendTimeout: time.Second,
	})

	w.enqueue(t.Context(), Event{EventType: EventSessionStart, SessionID: "a"})
	w.close(t.Context())

	require.Positive(t, w.failed.Load(), "the send was expected to fail")

	// net/http wraps transport failures in *url.Error, whose Error() embeds the
	// full URL. Logging that would put the write secret into Datadog on every
	// Stream outage, defeating the sensitive:"true" tag on the config field.
	messages := logger.AllMessages()
	assert.NotContains(t, messages, secret)
	assert.Contains(t, messages, "cannot send session event")
}

func TestWriter_DoesNotLogTheStreamSecretOnMalformedURL(t *testing.T) {
	t.Parallel()

	const secret = "THIS-IS-THE-WRITE-SECRET"

	logger := log.NewDebugLogger()
	// A control character makes http.NewRequest fail, which also yields a
	// *url.Error carrying the raw URL.
	w := newWriter(logger, writerConfig{
		url:         "http://127.0.0.1:1/stream/\x7f/" + secret,
		queueSize:   4,
		workers:     1,
		sendTimeout: time.Second,
	})

	w.enqueue(t.Context(), Event{EventType: EventSessionStart, SessionID: "a"})
	w.close(t.Context())

	require.Positive(t, w.failed.Load())
	assert.NotContains(t, logger.AllMessages(), secret)
}
