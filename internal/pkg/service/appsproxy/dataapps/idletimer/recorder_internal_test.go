package idletimer

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRecordActivity_WritesTheSharedRecordAtTheHeartbeatCadence(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	rec, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

func TestRecordActivity_DoesNotWriteWithinOneHeartbeatInterval(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	before, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)

	h.clock.Advance(time.Second)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	after, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.Equal(t, before.resourceVersion, after.resourceVersion, "a second request inside the interval must not cost a write")
}

// Activity for a workload that never auto-suspends has nothing to record.
func TestRecordActivity_WritesNothingWhenTheWorkloadHasNoThreshold(t *testing.T) {
	t.Parallel()

	h := newHarness(t, optedOut(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())
	h.fake.ClearActions()

	h.clock.Advance(time.Hour)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	assert.Empty(t, h.fake.Actions())
}

// The request path must not wait for the apiserver: this runs in the GotConn
// callback and, per websocket frame, in a callback the proxy documents as
// non-blocking.
func TestRecordActivity_DoesNotBlockTheRequestPath(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	release := make(chan struct{})
	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		<-release
		return false, nil, nil
	})

	h.clock.Advance(time.Minute)
	returned := make(chan struct{})
	go func() {
		h.recorder.RecordActivity(t.Context(), appRef)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("RecordActivity blocked on the K8s write")
	}

	close(release)
	h.flush()
}

// A write that failed did not refresh the shared record, so the next request
// must try again rather than wait out a full interval. Waiting would let the
// record fall two intervals behind while the margin covers one, and the other
// replica, which has none of this traffic in memory, would suspend early.
func TestRecordActivity_AFailedWriteDoesNotHoldOffTheNextOne(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	var fail atomic.Bool
	fail.Store(true)
	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() {
			return true, nil, k8serrors.NewInternalError(errors.New("apiserver is unwell"))
		}
		return false, nil, nil
	})

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	// Well inside one heartbeat interval, so without the reset this writes nothing.
	fail.Store(false)
	h.clock.Advance(time.Second)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	rec, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want the retry to land %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// A websocket frame can arrive after Shutdown begins, because hijacked
// connections are not drained by the HTTP server.
// A redeploy moves the App onto a new member Sandbox, and the record is named
// after the member. The cadence is a property of the record, so carrying it
// across the switch leaves the new record unwritten while traffic flows.
func TestRecordActivity_AMemberSwitchWritesTheNewRecordAtOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	h.source.candidates = []k8sapp.SleepCandidate{candidate(appRef, "member-2")}
	h.suspender.suspendIdleWorkloads(t.Context())

	// Well inside one heartbeat interval of the write to member-1.
	h.clock.Advance(time.Second)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	rec, err := h.recorder.client.get(t.Context(), "member-2")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want member-2 to carry %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// A failure a retry cannot clear must cost a bounded number of attempts, not
// one per request and one per websocket frame.
func TestRecordActivity_APersistentFailureIsNotRetriedPerRequest(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := newGatedHarnessWithLogger(t, logger, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: resource}, "member-1", errors.New("no patch rule"))
	})

	h.clock.Advance(time.Minute)
	h.fake.ClearActions()
	for range 50 {
		h.recorder.RecordActivity(t.Context(), appRef)
		h.flush()
	}

	var patches int
	for _, a := range h.fake.Actions() {
		if a.GetVerb() == "patch" {
			patches++
		}
	}
	assert.LessOrEqual(t, patches, 5, "a standing failure must not be retried once per request")
	assert.LessOrEqual(t, strings.Count(logger.AllMessages(), "failed to record activity"), 1,
		"the request path must not log once per request")
}

// A mark taken just before the process goes down still belongs in the shared
// record: the other replica reads it, and would otherwise see this workload as
// idle for however long the mark was worth.
func TestShutdown_RunsAFinalSweep(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())
	h.clock.Advance(time.Minute)
	h.startSweeper()

	h.recorder.RecordActivity(t.Context(), appRef)
	h.recorder.Shutdown(t.Context())

	rec, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want the final sweep to write %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// A workload can leave the cache while a request for it is still in flight.
func TestShutdown_IsNotHeldUpByAnEvictionRacingARequest(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())
	h.startSweeper()

	h.recorder.RecordActivity(t.Context(), appRef)
	h.recorder.Forget(appRef)
	h.recorder.RecordActivity(t.Context(), appRef)

	done := make(chan struct{})
	go func() {
		h.recorder.Shutdown(t.Context())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return")
	}
}

func TestShutdown_IsSafeToCallTwice(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.startSweeper()

	h.recorder.Shutdown(t.Context())
	assert.NotPanics(t, func() { h.recorder.Shutdown(t.Context()) })
}

// The write that matters most is the last one before a workload goes quiet, and
// nothing will arrive to prompt a retry of it. Retrying only when traffic nudges
// leaves the shared record frozen, and the other replica suspends on it.
func TestSweep_RetriesAfterTheTrafficHasStopped(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	var fail atomic.Bool
	fail.Store(true)
	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() {
			return true, nil, k8serrors.NewInternalError(errors.New("apiserver is unwell"))
		}
		return false, nil, nil
	})

	h.clock.Advance(time.Minute)
	at := h.clock.Now()
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	// Not one more request from here on.
	fail.Store(false)
	h.clock.Advance(writeRetryInterval + time.Second)
	h.flush()

	rec, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, at.Equal(rec.lastRequestAt), "want the retry to land %s, got %s", at, rec.lastRequestAt)
}

// A standing failure is the one thing that can suspend a workload in use, and
// the logs alone do not put it on a dashboard.
func TestSweep_CountsAFailedWriteByReason(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: resource}, "member-1", errors.New("no patch rule"))
	})

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	assert.Equal(t, int64(1), h.counterValue(t, "keboola.go.appsproxy.idletimer.write_failures"))
}

// The loop has two ways to end and the record is shared, so a mark must reach
// the apiserver whichever one fires. Which case a ready select takes is not
// defined, so the ctx one has to sweep as well.
func TestRun_SweepsWhateverEndsTheLoop(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		stop func(*harness, context.CancelCauseFunc)
	}{
		{"context cancelled", func(_ *harness, cancel context.CancelCauseFunc) { cancel(errors.New("stop")) }},
		{"shutdown", func(h *harness, _ context.CancelCauseFunc) {
			h.recorder.stopOnce.Do(func() { close(h.recorder.stopping) })
		}},
		{"both at once", func(h *harness, cancel context.CancelCauseFunc) {
			cancel(errors.New("stop"))
			h.recorder.stopOnce.Do(func() { close(h.recorder.stopping) })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t, candidate(appRef, "member-1"))
			h.suspender.suspendIdleWorkloads(t.Context())

			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(errors.New("test done"))

			h.clock.Advance(time.Minute)
			h.recorder.RecordActivity(ctx, appRef)
			want := h.clock.Now()

			tc.stop(h, cancel)
			h.recorder.run(ctx)

			rec, err := h.recorder.client.get(t.Context(), "member-1")
			require.NoError(t, err)
			assert.True(t, want.Equal(rec.lastRequestAt), "want the final sweep to write %s, got %s", want, rec.lastRequestAt)
		})
	}
}
