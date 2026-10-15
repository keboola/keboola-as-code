package idletimer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRound_SkipsEverythingBeforeTheCacheHasSynced(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.source.synced = false

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept)
	for _, action := range h.fake.Actions() {
		assert.NotEqual(t, "create", action.GetVerb(), "an unsynced cache must not be acted on")
	}
}

func TestRound_AbsentThresholdIsSkippedAndCounted(t *testing.T) {
	t.Parallel()

	h := newHarness(t, optedOut(appRef))

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept)
	_, err := h.recorder.client.get(t.Context(), testMember)
	require.NoError(t, err, "a threshold can be set at any moment, so the record must already exist")
	assert.Equal(t, int64(1), h.suspender.metrics.noThreshold.Load())
	assert.Zero(t, h.suspender.metrics.candidates.Load())
}

// The gauge reads the current round, not a running total: one transient skip
// must not leave it permanently non-zero. A workload moves between states
// rather than accumulating in both.
func TestRound_TheWorkloadsGaugeReportsTheCurrentRound(t *testing.T) {
	t.Parallel()

	h := newHarness(t, optedOut(appRef))
	h.suspender.suspendIdleWorkloads(t.Context())
	require.Equal(t, int64(1), h.suspender.metrics.noThreshold.Load())

	h.source.candidates[0] = candidate(appRef, "member-1")
	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Zero(t, h.suspender.metrics.noThreshold.Load())
	assert.Equal(t, int64(1), h.suspender.metrics.candidates.Load())
}

// An App whose member cannot be resolved never suspends either, and it is its
// own state: a bug smell, unlike a workload that merely opted out.
func TestRound_UnresolvedWorkloadsAreReportedSeparately(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.source.unresolved = []api.AppID{"a", "b"}

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Equal(t, int64(2), h.suspender.metrics.unresolved.Load())
	assert.Zero(t, h.suspender.metrics.noThreshold.Load())
}

func TestRound_AbsentRecordIsCreatedAtNowAndNotSuspended(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept, "a record created this round gives a full grace window")

	rec, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// An apiserver wobble must not read as "never requested": only NotFound is
// absence, anything else means skip the round and try again.
func TestRound_AReadErrorIsNotAbsence(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewInternalError(errors.New("apiserver is unwell"))
	})

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept)
	for _, action := range h.fake.Actions() {
		assert.NotEqual(t, "create", action.GetVerb(), "an error must not be mistaken for absence")
	}
}

func TestRound_SuspendsAWorkloadIdlePastTheMargin(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))

	// The first round creates the record.
	h.round()
	require.Empty(t, h.source.slept)

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()

	assert.Equal(t, []k8sapp.WorkloadRef{appRef}, h.source.slept)
	assert.Equal(t, int64(1), h.suspendCount(t, outcomePerformed))
}

func TestRound_DoesNotSuspendBeforeTheThreshold(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(threshold)
	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept, "a workload must never be suspended before its threshold")
}

// In-memory activity on this replica counts even when the shared record is
// older, so a replica serving traffic does not suspend what it is serving.
func TestRound_InMemoryActivityHoldsAWorkloadAwake(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(threshold)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()
	h.clock.Advance(heartbeatInterval(threshold) + time.Second)
	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept)
}

// A workload restarted while its record survived must get a full window, not
// be judged against a timestamp from before it came back.
func TestRound_ARestartCountsAsActivity(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.source.candidates[0].LastStarted = h.clock.Now()
	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept)
}

func TestRound_SuspendsADraftThroughItsOwnWorkload(t *testing.T) {
	t.Parallel()

	draftRef := k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}
	h := newHarness(t, candidate(draftRef, "draft-abc"))

	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()

	assert.Equal(t, []k8sapp.WorkloadRef{draftRef}, h.source.slept)
}

func TestRound_CountsASuspendFollowedByAWakeWithinAMinute(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()
	require.Equal(t, int64(1), h.suspendCount(t, outcomePerformed))

	// Something woke it: the workload started again, which moves lastStartedTime.
	h.clock.Advance(30 * time.Second)
	h.source.candidates[0].LastStarted = h.clock.Now()
	h.round()

	assert.Equal(t, int64(1), h.counterValue(t, "keboola.go.appsproxy.idletimer.woke_soon_after_suspend"))
}

// A patched workload stays Running in the cache until the operator reconciles
// it and the informer delivers the change. That is this loop working, not
// failing, and counting it would drown the signal the metric exists to carry.
func TestRound_DoesNotCountAnUnreconciledSuspendAsAWake(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()
	require.Equal(t, int64(1), h.suspendCount(t, outcomePerformed))

	// Still in the Running set a round later, but it never restarted.
	h.clock.Advance(15 * time.Second)
	h.round()

	assert.Zero(t, h.counterValue(t, "keboola.go.appsproxy.idletimer.woke_soon_after_suspend"))
}

func TestRound_DoesNotCountAWakeLongAfterTheSuspend(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.suspender.suspendIdleWorkloads(t.Context())
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.suspender.suspendIdleWorkloads(t.Context())

	h.clock.Advance(2 * time.Minute)
	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Zero(t, h.counterValue(t, "keboola.go.appsproxy.idletimer.woke_soon_after_suspend"))
}

func TestRound_GateSuppressesOnlyTheSuspendAction(t *testing.T) {
	t.Parallel()

	h := newGatedHarness(t, candidate(appRef, "member-1"))
	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()

	assert.Empty(t, h.source.slept, "the gate must stop the patch")
	assert.Zero(t, h.suspendCount(t, outcomePerformed))
	assert.Equal(t, int64(1), h.suspendCount(t, outcomeSuppressed))

	// Everything leading up to the suspend still has to run: that is what the
	// gated deploy exists to exercise against a real apiserver.
	rec, err := h.recorder.client.get(t.Context(), "member-1")
	require.NoError(t, err)
	assert.False(t, rec.lastRequestAt.IsZero(), "the record must still be created")
	assert.Equal(t, int64(1), h.suspender.metrics.candidates.Load())
}

// A suppressed suspend leaves the workload Running, so every later round decides
// to suspend it again. Reporting each decision would be four lines a minute for
// every idle workload on the stack.
func TestRound_SuppressedSuspendIsReportedOncePerIdleEpisode(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := newGatedHarnessWithLogger(t, logger, candidate(appRef, "member-1"))
	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)

	for range 5 {
		h.round()
		h.clock.Advance(tickInterval)
	}

	assert.Equal(t, int64(1), h.suspendCount(t, outcomeSuppressed))
	assert.Equal(t, 1, strings.Count(logger.AllMessages(), "would suspend"))
}

// A workload that came back and went idle again is a second episode, and the
// comparison against the cron counts it separately.
func TestRound_ASecondIdleEpisodeIsReportedAgain(t *testing.T) {
	t.Parallel()

	h := newGatedHarness(t, candidate(appRef, "member-1"))
	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()
	require.Equal(t, int64(1), h.suspendCount(t, outcomeSuppressed))

	// Someone used it again.
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()
	h.round()

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()

	assert.Equal(t, int64(2), h.suspendCount(t, outcomeSuppressed))
}

func TestRound_SuppressedSuspendNamesTheWorkloadAndHowLongItWasIdle(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := newGatedHarnessWithLogger(t, logger, candidate(appRef, "member-1"))
	h.round()
	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.round()

	messages := logger.AllMessages()
	assert.Contains(t, messages, "would suspend")
	assert.Contains(t, messages, appRef.String())
	assert.Contains(t, messages, "idle for")
}

// Both replicas run rounds, so both can try to create the same record. The loser is
// not an error, and counting it as one would put noise into the metric that
// gates retiring the cron.
func TestRound_AlreadyExistsIsNotAnError(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := buildHarness(t, logger, true,
		[]k8sapp.SleepCandidate{candidate(appRef, "member-1")})
	h.fake.PrependReactor("create", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewAlreadyExists(gvr().GroupResource(), "member-1")
	})

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.NotContains(t, logger.AllMessages(), "failed to create the idle timer",
		"the other replica winning the race is this outcome, not a failure")
}

func TestRound_PassesTheSandboxUIDToTheRecord(t *testing.T) {
	t.Parallel()

	c := candidate(appRef, "member-1")
	c.SandboxUID = "member-uid-1"
	h := newHarness(t, c)

	h.suspender.suspendIdleWorkloads(t.Context())

	obj, err := h.fake.Resource(gvr()).Namespace(testNamespace).Get(t.Context(), "member-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, obj.GetOwnerReferences(), 1)
	assert.Equal(t, k8stypes.UID("member-uid-1"), obj.GetOwnerReferences()[0].UID)
}

// A standing misconfiguration would otherwise log on every round, for as long as
// it lasts.
func TestRound_WarnsOncePerUnresolvedApp(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := buildHarness(t, logger, true, nil)
	h.source.unresolved = []api.AppID{"123"}

	for range 3 {
		h.suspender.suspendIdleWorkloads(t.Context())
	}

	assert.Equal(t, 1, strings.Count(logger.AllMessages(), "productionSandbox"))
	assert.Equal(t, int64(1), h.suspender.metrics.unresolved.Load())
}

// A missing RBAC rule or CRD fails for every workload on every round. The PR
// calls that a safe state, which it is not if it also buries the logs.
func TestRound_RepeatedReadFailureIsLoggedOnce(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	h := buildHarness(t, logger, true,
		[]k8sapp.SleepCandidate{candidate(appRef, "member-1")})
	h.fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(gvr().GroupResource(), "member-1", errors.New("no rbac"))
	})

	for range 4 {
		h.suspender.suspendIdleWorkloads(t.Context())
	}

	assert.Equal(t, 1, strings.Count(logger.AllMessages(), "failed to read the idle timer"))

	// Once and then silent would be worse than too loud: this log is the only
	// signal that the records are failing, so it has to keep saying so.
	h.clock.Advance(recordErrorLogInterval + time.Second)
	h.suspender.suspendIdleWorkloads(t.Context())
	assert.Equal(t, 2, strings.Count(logger.AllMessages(), "failed to read the idle timer"),
		"a failure that outlives the interval must be logged again")
}

// A round that ran out of time must stop, not spend the rest of the round
// turning a cancelled context into one failure per remaining workload.
func TestRound_StopsWhenTheRoundRunsOutOfTime(t *testing.T) {
	t.Parallel()

	h := newHarness(t,
		candidate(appRef, "member-1"),
		candidate(k8sapp.WorkloadRef{AppID: "456"}, "member-2"),
	)
	h.fake.PrependReactor("get", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("round out of time"))
	h.suspender.suspendIdleWorkloads(ctx)

	// Not "nothing was suspended": that holds even without the break, because
	// the reactor fails every read. The round must not touch the apiserver.
	assert.Empty(t, h.fake.Actions(), "a cancelled round must stop, not visit the rest")
}

// Shadow mode exists to justify turning the gate on, so it has to count what
// the enabled loop would do. A cached read that the apiserver disagrees with is
// a suspend that would not have happened.
func TestRound_AGatedDecisionIsConfirmedBeforeItIsCounted(t *testing.T) {
	t.Parallel()

	h := newGatedHarness(t, candidate(appRef, "member-1"))
	require.NoError(t, h.recorder.client.create(t.Context(), "member-1", "uid-1", h.clock.Now()))

	// The cache still holds a value from long before the last request.
	h.cacheRecord("member-1", h.clock.Now().Add(-2*threshold))
	h.clock.Advance(time.Second)

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Zero(t, h.suspendCount(t, outcomeSuppressed), "a stale cached read must not be counted as a suspend that would have happened")
}

// A pod is Ready, and taking traffic, before its first tick. Its marks reach
// the shared record only once a round has told the Recorder which record they
// belong to, so until then the other replica cannot see them.
func TestRun_NamesEveryRecordBeforeTheFirstTick(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, testMember))

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(errors.New("test done"))
	go h.suspender.run(ctx)

	require.Eventually(t, func() bool {
		_, err := h.recorder.client.get(t.Context(), testMember)
		return err == nil
	}, 2*time.Second, 5*time.Millisecond, "no round ran before the first tick")

	h.clock.Advance(time.Second)
	h.recorder.RecordActivity(t.Context(), appRef)
	h.flush()

	rec, err := h.recorder.client.get(t.Context(), testMember)
	require.NoError(t, err)
	assert.True(t, h.clock.Now().Equal(rec.lastRequestAt), "want %s, got %s", h.clock.Now(), rec.lastRequestAt)
}

// A replica whose own writes stop landing cannot tell idleness from a record
// nobody can update. At the shortest threshold its entry goes overdue well
// before a suspend could be due.
func TestRound_DoesNotSuspendWhileItsOwnWritesAreFailing(t *testing.T) {
	t.Parallel()

	h := newHarness(t, withThreshold(appRef, minThreshold))
	h.round()

	h.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(gvr().GroupResource(), testMember, errors.New("patch is denied"))
	})

	h.clock.Advance(minThreshold + heartbeatInterval(minThreshold) + time.Second)
	h.round()

	assert.Empty(t, h.source.slept)
}
