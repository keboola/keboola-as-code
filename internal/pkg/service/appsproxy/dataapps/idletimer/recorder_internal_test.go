package idletimer

import (
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
	h.manager.suspendIdleWorkloads(t.Context())

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
	h.manager.suspendIdleWorkloads(t.Context())

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
	h.manager.suspendIdleWorkloads(t.Context())
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
	h.manager.suspendIdleWorkloads(t.Context())

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
	h.manager.suspendIdleWorkloads(t.Context())

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
func TestShutdown_StopsAdmittingWrites(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.manager.suspendIdleWorkloads(t.Context())
	before := len(h.fake.Actions())

	h.recorder.Shutdown(t.Context())

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	assert.Len(t, h.fake.Actions(), before, "a write admitted after Shutdown would race its Wait")
}

// A redeploy moves the App onto a new member Sandbox, and the record is named
// after the member. The cadence is a property of the record, so carrying it
// across the switch leaves the new record unwritten while traffic flows.
func TestRecordActivity_AMemberSwitchWritesTheNewRecordAtOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.manager.suspendIdleWorkloads(t.Context())

	h.clock.Advance(time.Minute)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	h.source.candidates = []k8sapp.SleepCandidate{candidate(appRef, "member-2")}
	h.manager.suspendIdleWorkloads(t.Context())

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
	h.manager.suspendIdleWorkloads(t.Context())

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
