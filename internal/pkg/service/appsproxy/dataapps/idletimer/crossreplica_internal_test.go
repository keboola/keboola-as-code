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
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Lowering a threshold shortens the window the reader judges by, but the
// record was written under the old one and may be a whole old heartbeat
// behind. The replica with no traffic of its own then reads a workload that
// took a request moments ago as idle.
func TestCrossReplica_LoweringTheThresholdSuspendsAWorkloadInUse(t *testing.T) {
	t.Parallel()

	const (
		oldThreshold = time.Hour       // heartbeat 180s
		newThreshold = 2 * time.Minute // heartbeat 6s
	)

	serving, idle := newReplicaPair(t, withThreshold(appRef, oldThreshold))
	serving.round()
	idle.round()

	serving.recorder.RecordActivity(t.Context(), appRef)
	serving.flush()

	// Traffic keeps arriving, but under the old threshold nothing is owed for
	// another 180s, so the record stays where it was.
	serving.clock.Advance(170 * time.Second)
	serving.recorder.RecordActivity(t.Context(), appRef)
	serving.flush()
	idle.flush()

	// The threshold is lowered. The idle replica sees it first.
	serving.source.candidates[0] = withThreshold(appRef, newThreshold)
	idle.source.candidates[0] = withThreshold(appRef, newThreshold)

	idle.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, idle.source.slept, "the workload took a request now; a lowered threshold must not suspend it on a record written under the old one")
}

// With no threshold the record is written only at the longest cadence. Setting
// a short one must not read that trail as idleness.
func TestCrossReplica_ReenablingAThresholdSuspendsAWorkloadInUse(t *testing.T) {
	t.Parallel()

	serving, idle := newReplicaPair(t, optedOut(appRef))
	serving.round()
	idle.round()

	// Traffic every minute while the threshold is absent.
	for range 20 {
		serving.clock.Advance(time.Minute)
		serving.recorder.RecordActivity(t.Context(), appRef)
		serving.round()
		idle.round()
	}

	// More traffic, inside one longest heartbeat of the last write, so the
	// record trails it.
	for range 4 {
		serving.clock.Advance(time.Minute)
		serving.recorder.RecordActivity(t.Context(), appRef)
		serving.flush()
		idle.flush()
	}

	// A short threshold is set, and traffic never stopped.
	serving.source.candidates[0] = withThreshold(appRef, time.Minute)
	idle.source.candidates[0] = withThreshold(appRef, time.Minute)

	idle.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, idle.source.slept, "the record trailed by the heartbeat it was written under, not because the workload was idle")
}

// A write failure that outlives the threshold freezes the shared record. The
// replica that serves the traffic knows better from memory; the other one has
// only the record, and the live confirm reads that same frozen record.
func TestCrossReplica_AStandingWriteFailureSuspendsAWorkloadInUse(t *testing.T) {
	t.Parallel()

	serving, idle := newReplicaPair(t, candidate(appRef, testMember))
	serving.round()
	idle.round()

	serving.recorder.RecordActivity(t.Context(), appRef)
	serving.flush()

	// patch is denied, get and create still work: one missing RBAC verb. Both
	// replicas run as the same ServiceAccount, so it is denied to both.
	serving.fake.PrependReactor("patch", resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(gvr().GroupResource(), testMember, errors.New("patch is denied"))
	})

	// Traffic keeps arriving for longer than the threshold.
	for range 12 {
		serving.clock.Advance(100 * time.Second)
		serving.recorder.RecordActivity(t.Context(), appRef)
		serving.flush()
		idle.flush()
	}
	require.NotZero(t, serving.counterValue(t, "keboola.go.appsproxy.idletimer.write_failures"), "the setup must actually fail writes")

	idle.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, idle.source.slept, "the record is frozen by a write failure, not by idleness")
}

// Both replicas take traffic, as they do behind a Service with no affinity.
// The one that decides cannot see the other's mark, which the old heartbeat
// leaves unwritten.
func TestCrossReplica_BothServingAndTheThresholdIsLowered(t *testing.T) {
	t.Parallel()

	a, b := newReplicaPair(t, withThreshold(appRef, time.Hour))
	a.round()
	b.round()

	b.recorder.RecordActivity(t.Context(), appRef)
	b.flush()

	a.clock.Advance(90 * time.Second)
	a.recorder.RecordActivity(t.Context(), appRef)
	a.flush()

	// b's next mark is inside its heartbeat, so it stays in b's memory.
	a.clock.Advance(80 * time.Second)
	b.recorder.RecordActivity(t.Context(), appRef)
	b.flush()
	a.flush()

	a.source.candidates[0] = withThreshold(appRef, time.Minute)
	b.source.candidates[0] = withThreshold(appRef, time.Minute)

	a.clock.Advance(15 * time.Second)
	a.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, a.source.slept, "the other replica served this workload 15s ago")
}

// A write that lands late carries an old mark under a fresh aliveAt, and a
// mark taken after it is not yet due. Reading the span from that mark to the
// oldest aliveAt as confirmed silence would suspend a workload in use.
func TestCrossReplica_AWriteThatLandsLateDoesNotHideALaterMark(t *testing.T) {
	t.Parallel()

	a, b := newReplicaPair(t, withThreshold(appRef, time.Hour))
	a.round()
	b.round()
	start := a.clock.Now()

	var failures atomic.Int32
	a.fake.PrependReactor("patch", resource, func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failures.Load() < 6 && strings.Contains(string(action.(k8stesting.PatchAction).GetPatch()), `"apps-proxy-a"`) {
			failures.Add(1)
			return true, nil, k8serrors.NewServerTimeout(gvr().GroupResource(), "patch", 1)
		}
		return false, nil, nil
	})

	a.clock.Advance(time.Minute)
	a.recorder.RecordActivity(t.Context(), appRef)
	mark := a.clock.Now()
	for range 200 {
		a.flush()
		if rec, err := a.recorder.client.get(t.Context(), testMember); err == nil && rec.lastRequestAt.Equal(mark) {
			break
		}
		a.clock.Advance(time.Second)
	}
	require.GreaterOrEqual(t, a.clock.Since(mark), time.Minute, "the setup must land the write late")

	// b's next heartbeat comes after a's late write landed, so the oldest
	// aliveAt is a's.
	a.clock.Advance(start.Add(heartbeatInterval(time.Hour)).Sub(a.clock.Now()))
	b.flush()

	// A later mark on a, inside the heartbeat of the value that landed.
	a.clock.Advance(5 * time.Second)
	a.recorder.RecordActivity(t.Context(), appRef)
	a.flush()

	a.source.candidates[0] = withThreshold(appRef, time.Minute)
	b.source.candidates[0] = withThreshold(appRef, time.Minute)

	a.clock.Advance(5 * time.Second)
	b.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, b.source.slept, "replica a served this workload 5s ago")
}

// A pod that is gone stops writing. Once its entry is overdue its heartbeat no
// longer widens the margin, so a workload idle under everyone still writing is
// suspended.
func TestCrossReplica_AnOverdueEntryNoLongerHoldsTheMargin(t *testing.T) {
	t.Parallel()

	gone, live := newReplicaPair(t, withThreshold(appRef, time.Hour))
	gone.round()
	live.round()

	// The threshold drops. The gone pod declared 180s and never writes again.
	live.source.candidates[0] = candidate(appRef, testMember)
	live.round()

	live.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	live.round()

	assert.Equal(t, []k8sapp.WorkloadRef{appRef}, live.source.slept)
}
