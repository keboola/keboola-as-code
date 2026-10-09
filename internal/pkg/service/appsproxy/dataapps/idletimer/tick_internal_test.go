package idletimer

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const threshold = 900 * time.Second

var appRef = k8sapp.WorkloadRef{AppID: "123"} //nolint:gochecknoglobals // test fixture

// fakeSource stands in for the K8s state watcher.
type fakeSource struct {
	synced     bool
	unresolved []api.AppID
	candidates []k8sapp.SleepCandidate
	slept      []k8sapp.WorkloadRef
	sleptOK    bool
	sleepErr   error
}

func (f *fakeSource) HasSynced() bool { return f.synced }

func (f *fakeSource) ScanForSleepCandidates() k8sapp.SleepScan {
	return k8sapp.SleepScan{Candidates: f.candidates, Unresolved: f.unresolved}
}

func (f *fakeSource) Sleep(_ context.Context, ref k8sapp.WorkloadRef, _ string) (bool, error) {
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
	tel     telemetry.ForTest
}

// counterValue reads an emitted counter back, so the assertions are about what
// a dashboard would show rather than about a field kept for the tests.
// suspendCount reads one outcome of the suspends counter. Taking the first
// data point would read whichever outcome happened to be recorded first.
func (h *harness) suspendCount(t *testing.T, outcome string) int64 {
	t.Helper()
	for _, m := range h.tel.Metrics(t) {
		if m.Name != "keboola.go.appsproxy.idletimer.suspends" {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			return 0
		}
		for _, dp := range sum.DataPoints {
			if v, found := dp.Attributes.Value(attribute.Key("outcome")); found && v.AsString() == outcome {
				return dp.Value
			}
		}
	}
	return 0
}

// flush waits for the detached writes. Shutdown also waits, but it stops
// admitting new ones, so a test that writes again afterwards cannot use it.
func (h *harness) flush() {
	h.manager.wg.Wait()
}

func (h *harness) counterValue(t *testing.T, name string) int64 {
	t.Helper()
	for _, m := range h.tel.Metrics(t) {
		if m.Name != name {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok || len(sum.DataPoints) == 0 {
			return 0
		}
		return sum.DataPoints[0].Value
	}
	return 0
}

// newHarness builds a manager with the suspend action on, which is what the
// loop's own behaviour has to be correct for once the gate is flipped.
func newHarness(t *testing.T, candidates ...k8sapp.SleepCandidate) *harness {
	t.Helper()
	return buildHarness(t, log.NewNopLogger(), true, candidates)
}

func newGatedHarness(t *testing.T, candidates ...k8sapp.SleepCandidate) *harness {
	t.Helper()
	return buildHarness(t, log.NewNopLogger(), false, candidates)
}

func newGatedHarnessWithLogger(t *testing.T, logger log.Logger, candidates ...k8sapp.SleepCandidate) *harness {
	t.Helper()
	return buildHarness(t, logger, false, candidates)
}

func buildHarness(t *testing.T, logger log.Logger, suspendEnabled bool, candidates []k8sapp.SleepCandidate) *harness {
	t.Helper()

	c := newTestClient()
	source := &fakeSource{synced: true, candidates: candidates, sleptOK: true}
	clock := clockwork.NewFakeClockAt(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))
	tel := telemetry.NewForTest(t)

	return &harness{
		manager: newManager(clock, logger, c, source, newMetrics(tel.Meter()), suspendEnabled),
		source:  source,
		clock:   clock,
		fake:    c.dyn.(*k8sfake.FakeDynamicClient),
		tel:     tel,
	}
}

func candidate(ref k8sapp.WorkloadRef, sandbox string, t time.Duration) k8sapp.SleepCandidate {
	return k8sapp.SleepCandidate{Ref: ref, SandboxName: sandbox, Threshold: t}
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
	assert.Equal(t, int64(1), h.manager.metrics.noThreshold.Load())
	assert.Zero(t, h.manager.metrics.candidates.Load())
}

// The gauge reads the current round, not a running total: one transient skip
// must not leave it permanently non-zero. A workload moves between states
// rather than accumulating in both.
func TestTick_TheWorkloadsGaugeReportsTheCurrentRound(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", 0))
	h.manager.tick(t.Context())
	require.Equal(t, int64(1), h.manager.metrics.noThreshold.Load())

	h.source.candidates[0].Threshold = threshold
	h.manager.tick(t.Context())

	assert.Zero(t, h.manager.metrics.noThreshold.Load())
	assert.Equal(t, int64(1), h.manager.metrics.candidates.Load())
}

// An App whose member cannot be resolved never suspends either, and it is its
// own state: a bug smell, unlike a workload that merely opted out.
func TestTick_UnresolvedWorkloadsAreReportedSeparately(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.source.unresolved = []api.AppID{"a", "b"}

	h.manager.tick(t.Context())

	assert.Equal(t, int64(2), h.manager.metrics.unresolved.Load())
	assert.Zero(t, h.manager.metrics.noThreshold.Load())
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
	assert.Equal(t, int64(1), h.suspendCount(t, outcomePerformed))
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
	h.flush()
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
	require.Equal(t, int64(1), h.suspendCount(t, outcomePerformed))

	// Something woke it: the workload started again, which moves lastStartedTime.
	h.clock.Advance(30 * time.Second)
	h.source.candidates[0].LastStarted = h.clock.Now()
	h.manager.tick(t.Context())

	assert.Equal(t, int64(1), h.counterValue(t, "keboola.go.appsproxy.idletimer.woke_soon_after_suspend"))
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
	require.Equal(t, int64(1), h.suspendCount(t, outcomePerformed))

	// Still in the Running set a tick later, but it never restarted.
	h.clock.Advance(15 * time.Second)
	h.manager.tick(t.Context())

	assert.Zero(t, h.counterValue(t, "keboola.go.appsproxy.idletimer.woke_soon_after_suspend"))
}

func TestTick_DoesNotCountAWakeLongAfterTheSuspend(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	h.clock.Advance(2 * time.Minute)
	h.manager.tick(t.Context())

	assert.Zero(t, h.counterValue(t, "keboola.go.appsproxy.idletimer.woke_soon_after_suspend"))
}

func TestRecordActivity_WritesTheSharedRecordAtTheHeartbeatCadence(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	h.clock.Advance(time.Minute)
	h.manager.RecordActivity(t.Context(), appRef)
	h.flush()

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
	h.flush()

	before, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)

	h.clock.Advance(time.Second)
	h.manager.RecordActivity(t.Context(), appRef)
	h.flush()

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
	h.flush()

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
	h.flush()
}

func TestTick_GateSuppressesOnlyTheSuspendAction(t *testing.T) {
	t.Parallel()

	h := newGatedHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept, "the gate must stop the patch")
	assert.Zero(t, h.suspendCount(t, outcomePerformed))
	assert.Equal(t, int64(1), h.suspendCount(t, outcomeSuppressed))

	// Everything leading up to the suspend still has to run: that is what the
	// gated deploy exists to exercise against a real apiserver.
	rec, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.False(t, rec.lastRequestAt.IsZero(), "the record must still be created")
	assert.Equal(t, int64(1), h.manager.metrics.candidates.Load())
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

	assert.Equal(t, int64(1), h.suspendCount(t, outcomeSuppressed))
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
	require.Equal(t, int64(1), h.suspendCount(t, outcomeSuppressed))

	// Someone used it again.
	h.manager.RecordActivity(t.Context(), appRef)
	h.flush()
	h.manager.tick(t.Context())

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Equal(t, int64(2), h.suspendCount(t, outcomeSuppressed))
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

	logger := log.NewDebugLogger()
	h := buildHarness(t, logger, true,
		[]k8sapp.SleepCandidate{candidate(appRef, "member-1", threshold)})
	h.fake.PrependReactor("create", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewAlreadyExists(gvr().GroupResource(), "member-1")
	})

	h.manager.tick(t.Context())

	assert.NotContains(t, logger.AllMessages(), "failed to create the idle timer",
		"the other replica winning the race is this outcome, not a failure")
}

func TestTick_PassesTheSandboxUIDToTheRecord(t *testing.T) {
	t.Parallel()

	c := candidate(appRef, "member-1", threshold)
	c.SandboxUID = "member-uid-1"
	h := newHarness(t, c)

	h.manager.tick(t.Context())

	obj, err := h.fake.Resource(gvr()).Namespace(testNamespace).Get(t.Context(), "member-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, obj.GetOwnerReferences(), 1)
	assert.Equal(t, k8stypes.UID("member-uid-1"), obj.GetOwnerReferences()[0].UID)
}

// A standing misconfiguration would otherwise log on every tick, for as long as
// it lasts.
func TestTick_WarnsOncePerUnresolvedApp(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := buildHarness(t, logger, true, nil)
	h.source.unresolved = []api.AppID{"123"}

	for range 3 {
		h.manager.tick(t.Context())
	}

	assert.Equal(t, 1, strings.Count(logger.AllMessages(), "productionSandbox"))
	assert.Equal(t, int64(1), h.manager.metrics.unresolved.Load())
}

// A write that failed did not refresh the shared record, so the next request
// must try again rather than wait out a full interval. Waiting would let the
// record fall two intervals behind while the margin covers one, and the other
// replica, which has none of this traffic in memory, would suspend early.
func TestRecordActivity_AFailedWriteDoesNotHoldOffTheNextOne(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())

	var fail atomic.Bool
	fail.Store(true)
	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() {
			return true, nil, k8serrors.NewInternalError(errors.New("apiserver is unwell"))
		}
		return false, nil, nil
	})

	h.clock.Advance(time.Minute)
	h.manager.RecordActivity(t.Context(), appRef)
	h.flush()

	// Well inside one heartbeat interval, so without the reset this writes nothing.
	fail.Store(false)
	h.clock.Advance(time.Second)
	h.manager.RecordActivity(t.Context(), appRef)
	h.flush()

	rec, err := h.manager.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want the retry to land %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// A missing RBAC rule or CRD fails for every workload on every tick. The PR
// calls that a safe state, which it is not if it also buries the logs.
func TestTick_RepeatedReadFailureIsLoggedOnce(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := buildHarness(t, logger, true,
		[]k8sapp.SleepCandidate{candidate(appRef, "member-1", threshold)})
	h.fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(gvr().GroupResource(), "member-1", errors.New("no rbac"))
	})

	for range 4 {
		h.manager.tick(t.Context())
	}

	assert.Equal(t, 1, strings.Count(logger.AllMessages(), "failed to read the idle timer"))

	// Once and then silent would be worse than too loud: this log is the only
	// signal that the records are failing, so it has to keep saying so.
	h.clock.Advance(recordErrorLogInterval + time.Second)
	h.manager.tick(t.Context())
	assert.Equal(t, 2, strings.Count(logger.AllMessages(), "failed to read the idle timer"),
		"a failure that outlives the interval must be logged again")
}

// A tick that ran out of time must stop, not spend the rest of the round
// turning a cancelled context into one failure per remaining workload.
func TestTick_StopsWhenTheRoundRunsOutOfTime(t *testing.T) {
	t.Parallel()

	h := newHarness(t,
		candidate(appRef, "member-1", threshold),
		candidate(k8sapp.WorkloadRef{AppID: "456"}, "member-2", threshold),
	)
	h.fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("round out of time"))
	h.manager.tick(ctx)

	// Not "nothing was suspended": that holds even without the break, because
	// the reactor fails every read. The round must not touch the apiserver.
	assert.Empty(t, h.fake.Actions(), "a cancelled round must stop, not visit the rest")
}

// A websocket frame can arrive after Shutdown begins, because hijacked
// connections are not drained by the HTTP server.
func TestShutdown_StopsAdmittingWrites(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1", threshold))
	h.manager.tick(t.Context())
	before := len(h.fake.Actions())

	h.manager.Shutdown(t.Context())

	h.clock.Advance(time.Minute)
	h.manager.RecordActivity(t.Context(), appRef)
	h.flush()

	assert.Len(t, h.fake.Actions(), before, "a write admitted after Shutdown would race its Wait")
}
