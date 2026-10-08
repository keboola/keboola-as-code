package idletimer

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const threshold = 900 * time.Second

var appRef = k8sapp.WorkloadRef{AppID: "123"} //nolint:gochecknoglobals // test fixture

// fakeSource stands in for the K8s state watcher.
type fakeSource struct {
	synced     bool
	candidates []k8sapp.SuspendCandidate
	slept      []k8sapp.WorkloadRef
	sleptOK    bool
	sleepErr   error
}

func (f *fakeSource) HasSynced() bool { return f.synced }

func (f *fakeSource) RunningWorkloads(context.Context) []k8sapp.SuspendCandidate {
	return f.candidates
}

func (f *fakeSource) Sleep(_ context.Context, ref k8sapp.WorkloadRef) (bool, error) {
	if f.sleepErr != nil {
		return false, f.sleepErr
	}
	f.slept = append(f.slept, ref)
	return f.sleptOK, nil
}

type harness struct {
	manager *Manager
	source  *fakeSource
	clock   *clockwork.FakeClock
	fake    *k8sfake.FakeDynamicClient
}

func newHarness(t *testing.T, candidates ...k8sapp.SuspendCandidate) *harness {
	t.Helper()

	c := newTestClient()
	source := &fakeSource{synced: true, candidates: candidates, sleptOK: true}
	clock := clockwork.NewFakeClockAt(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))

	return &harness{
		manager: newManager(clock, log.NewNopLogger(), c, source, newMetrics(nil)),
		source:  source,
		clock:   clock,
		fake:    c.dyn.(*k8sfake.FakeDynamicClient),
	}
}

func candidate(ref k8sapp.WorkloadRef, sandbox string, t time.Duration) k8sapp.SuspendCandidate {
	return k8sapp.SuspendCandidate{Ref: ref, SandboxName: sandbox, Threshold: t}
}

func TestTick_SkipsEverythingBeforeTheCacheHasSynced(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.source.synced = false

	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept)
	for _, action := range h.fake.Actions() {
		assert.NotEqual(t, "create", action.GetVerb(), "an unsynced cache must not be acted on")
	}
}

func TestTick_AbsentThresholdIsSkippedAndCounted(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", 0))

	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept)
	assert.Empty(t, h.fake.Actions(), "a workload that never auto-suspends needs no record")
	assert.Equal(t, int64(1), h.manager.metrics.skippedNoThreshold.Load())
}

func TestTick_AbsentRecordIsCreatedAtNowAndNotSuspended(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))

	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept, "a record created this tick gives a full grace window")

	rec, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// An apiserver wobble must not read as "never requested": only NotFound is
// absence, anything else means skip the round and try again.
func TestTick_AReadErrorIsNotAbsence(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewInternalError(errors.New("apiserver is unwell"))
	})

	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept)
	for _, action := range h.fake.Actions() {
		assert.NotEqual(t, "create", action.GetVerb(), "an error must not be mistaken for absence")
	}
	assert.Equal(t, int64(1), h.manager.metrics.recordErrors.Load())
}

func TestTick_SuspendsAWorkloadIdlePastTheMargin(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))

	// First tick creates the record.
	h.manager.tick(t.Context())
	require.Empty(t, h.source.slept)

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Equal(t, []k8sapp.WorkloadRef{appRef}, h.source.slept)
	assert.Equal(t, int64(1), h.manager.metrics.suspends.Load())
}

func TestTick_DoesNotSuspendBeforeTheThreshold(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	h.clock.Advance(threshold)
	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept, "a workload must never be suspended before its threshold")
}

// In-memory activity on this replica counts even when the shared record is
// older, so a replica serving traffic does not suspend what it is serving.
func TestTick_InMemoryActivityHoldsAWorkloadAwake(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	h.clock.Advance(threshold)
	h.manager.RecordActivity(t.Context(), appRef)
	h.manager.Shutdown(t.Context())
	h.clock.Advance(heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept)
}

// A workload restarted while its record survived must get a full window, not
// be judged against a timestamp from before it came back.
func TestTick_ARestartCountsAsActivity(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.source.candidates[0].LastStarted = h.clock.Now()
	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept)
}

func TestTick_SuspendsADraftThroughItsOwnWorkload(t *testing.T) {
	t.Parallel()

	draftRef := k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}
	h := newHarness(t, candidate(draftRef, "draft-abc", threshold))

	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Equal(t, []k8sapp.WorkloadRef{draftRef}, h.source.slept)
}

func TestTick_CountsASuspendFollowedByAWakeWithinAMinute(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())
	require.Equal(t, int64(1), h.manager.metrics.suspends.Load())

	// The workload is back in the Running set a moment later: something woke it.
	h.clock.Advance(30 * time.Second)
	h.manager.tick(t.Context())

	assert.Equal(t, int64(1), h.manager.metrics.wokeSoonAfterSuspend.Load())
}

func TestTick_DoesNotCountAWakeLongAfterTheSuspend(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	h.clock.Advance(2 * time.Minute)
	h.manager.tick(t.Context())

	assert.Zero(t, h.manager.metrics.wokeSoonAfterSuspend.Load())
}

func TestRecordActivity_WritesTheSharedRecordAtTheHeartbeatCadence(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	h.clock.Advance(time.Minute)
	h.manager.RecordActivity(t.Context(), appRef)
	h.manager.Shutdown(t.Context())

	rec, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

func TestRecordActivity_DoesNotWriteWithinOneHeartbeatInterval(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	h.clock.Advance(time.Minute)
	h.manager.RecordActivity(t.Context(), appRef)
	h.manager.Shutdown(t.Context())

	before, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)

	h.clock.Advance(time.Second)
	h.manager.RecordActivity(t.Context(), appRef)
	h.manager.Shutdown(t.Context())

	after, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.Equal(t, before.resourceVersion, after.resourceVersion, "a second request inside the interval must not cost a write")
}

// Activity for a workload that never auto-suspends has nothing to record.
func TestRecordActivity_WritesNothingWhenTheWorkloadHasNoThreshold(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", 0))
	h.manager.tick(t.Context())
	h.fake.ClearActions()

	h.clock.Advance(time.Hour)
	h.manager.RecordActivity(t.Context(), appRef)
	h.manager.Shutdown(t.Context())

	assert.Empty(t, h.fake.Actions())
}

// The request path must not wait for the apiserver: this runs in the GotConn
// callback and, per websocket frame, in a callback the proxy documents as
// non-blocking.
func TestRecordActivity_DoesNotBlockTheRequestPath(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	release := make(chan struct{})
	h.fake.PrependReactor("update", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		<-release
		return false, nil, nil
	})

	h.clock.Advance(time.Minute)
	returned := make(chan struct{})
	go func() {
		h.manager.RecordActivity(t.Context(), appRef)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("RecordActivity blocked on the K8s write")
	}

	close(release)
	h.manager.Shutdown(t.Context())
}
