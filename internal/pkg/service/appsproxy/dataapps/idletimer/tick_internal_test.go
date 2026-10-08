package idletimer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
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
	unresolved int
	candidates []k8sapp.SuspendCandidate
	slept      []k8sapp.WorkloadRef
	sleptOK    bool
	sleepErr   error
}

func (f *fakeSource) HasSynced() bool { return f.synced }

func (f *fakeSource) RunningWorkloads(context.Context) k8sapp.WorkloadSnapshot {
	return k8sapp.WorkloadSnapshot{Candidates: f.candidates, Unresolved: f.unresolved}
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

// newHarness builds a manager with the suspend action on, which is what the
// loop's own behaviour has to be correct for once the gate is flipped.
func newHarness(t *testing.T, candidates ...k8sapp.SuspendCandidate) *harness {
	t.Helper()
	return buildHarness(t, log.NewNopLogger(), true, candidates)
}

func newGatedHarness(t *testing.T, candidates ...k8sapp.SuspendCandidate) *harness {
	t.Helper()
	return buildHarness(t, log.NewNopLogger(), false, candidates)
}

func newGatedHarnessWithLogger(t *testing.T, logger log.Logger, candidates ...k8sapp.SuspendCandidate) *harness {
	t.Helper()
	return buildHarness(t, logger, false, candidates)
}

func buildHarness(t *testing.T, logger log.Logger, suspendEnabled bool, candidates []k8sapp.SuspendCandidate) *harness {
	t.Helper()

	c := newTestClient()
	source := &fakeSource{synced: true, candidates: candidates, sleptOK: true}
	clock := clockwork.NewFakeClockAt(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))

	return &harness{
		manager: newManager(clock, logger, c, source, newMetrics(nil), suspendEnabled),
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
	assert.Equal(t, int64(1), h.manager.metrics.notSuspendable.Load())
}

// The gate reads the current round, not a running total: one transient skip
// must not leave it permanently non-zero, or "zero for a full cycle" can never
// be satisfied again.
func TestTick_TheNotSuspendableGaugeReportsTheCurrentRound(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", 0))
	h.manager.tick(t.Context())
	require.Equal(t, int64(1), h.manager.metrics.notSuspendable.Load())

	h.source.candidates[0].Threshold = threshold
	h.manager.tick(t.Context())

	assert.Zero(t, h.manager.metrics.notSuspendable.Load())
}

// An App whose member cannot be resolved never suspends either, so it belongs
// in the same gate: otherwise the gate reads zero while workloads are silently
// being skipped.
func TestTick_UnresolvedWorkloadsCountTowardsTheGate(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.source.unresolved = 2

	h.manager.tick(t.Context())

	assert.Equal(t, int64(2), h.manager.metrics.notSuspendable.Load())
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

	// Something woke it: the workload started again, which moves lastStartedTime.
	h.clock.Advance(30 * time.Second)
	h.source.candidates[0].LastStarted = h.clock.Now()
	h.manager.tick(t.Context())

	assert.Equal(t, int64(1), h.manager.metrics.wokeSoonAfterSuspend.Load())
}

// A patched workload stays Running in the cache until the operator reconciles
// it and the informer delivers the change. That is this loop working, not
// failing, and counting it would drown the signal the metric exists to carry.
func TestTick_DoesNotCountAnUnreconciledSuspendAsAWake(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())
	require.Equal(t, int64(1), h.manager.metrics.suspends.Load())

	// Still in the Running set a tick later, but it never restarted.
	h.clock.Advance(15 * time.Second)
	h.manager.tick(t.Context())

	assert.Zero(t, h.manager.metrics.wokeSoonAfterSuspend.Load())
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
	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
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

// The constant ships off. Flipping it is a release decision, so a change here
// should be deliberate enough to need this test updated with it.
func TestSuspendEnabled_ShipsOff(t *testing.T) {
	t.Parallel()

	assert.False(t, suspendEnabled, "the suspend action is staged by release; flipping it is its own PR")
}

func TestTick_GateSuppressesOnlyTheSuspendAction(t *testing.T) {
	t.Parallel()

	h := newGatedHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept, "the gate must stop the patch")
	assert.Zero(t, h.manager.metrics.suspends.Load())
	assert.Equal(t, int64(1), h.manager.metrics.suspendsSuppressed.Load())

	// Everything leading up to the suspend still has to run: that is what the
	// gated deploy exists to exercise against a real apiserver.
	rec, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.False(t, rec.lastRequestAt.IsZero(), "the record must still be created")
	assert.Zero(t, h.manager.metrics.notSuspendable.Load())
}

// A suppressed suspend leaves the workload Running, so every later tick decides
// to suspend it again. Reporting each decision would be four lines a minute for
// every idle workload on the stack.
func TestTick_SuppressedSuspendIsReportedOncePerIdleEpisode(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := newGatedHarnessWithLogger(t, logger, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)

	for range 5 {
		h.manager.tick(t.Context())
		h.clock.Advance(tickInterval)
	}

	assert.Equal(t, int64(1), h.manager.metrics.suspendsSuppressed.Load())
	assert.Equal(t, 1, strings.Count(logger.AllMessages(), "would suspend"))
}

// A workload that came back and went idle again is a second episode, and the
// comparison against the cron counts it separately.
func TestTick_ASecondIdleEpisodeIsReportedAgain(t *testing.T) {
	t.Parallel()

	h := newGatedHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())
	require.Equal(t, int64(1), h.manager.metrics.suspendsSuppressed.Load())

	// Someone used it again.
	h.manager.RecordActivity(t.Context(), appRef)
	h.manager.Shutdown(t.Context())
	h.manager.tick(t.Context())

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Equal(t, int64(2), h.manager.metrics.suspendsSuppressed.Load())
}

func TestTick_SuppressedSuspendNamesTheWorkloadAndHowLongItWasIdle(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := newGatedHarnessWithLogger(t, logger, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	messages := logger.AllMessages()
	assert.Contains(t, messages, "would suspend")
	assert.Contains(t, messages, appRef.String())
	assert.Contains(t, messages, "idle for")
}

// Both replicas tick, so both can try to create the same record. The loser is
// not an error, and counting it as one would put noise into the metric that
// gates retiring the cron.
func TestTick_AlreadyExistsIsNotAnError(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.fake.PrependReactor("create", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewAlreadyExists(GVR().GroupResource(), "member-1")
	})

	h.manager.tick(t.Context())

	assert.Zero(t, h.manager.metrics.recordErrors.Load())
}

func TestTick_PassesTheSandboxUIDToTheRecord(t *testing.T) {
	t.Parallel()

	c := candidate(appRef, "member-1", threshold)
	c.SandboxUID = "member-uid-1"
	h := newHarness(t, c)

	h.manager.tick(t.Context())

	obj, err := h.fake.Resource(GVR()).Namespace(testNamespace).Get(t.Context(), "member-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, obj.GetOwnerReferences(), 1)
	assert.Equal(t, k8stypes.UID("member-uid-1"), obj.GetOwnerReferences()[0].UID)
}
