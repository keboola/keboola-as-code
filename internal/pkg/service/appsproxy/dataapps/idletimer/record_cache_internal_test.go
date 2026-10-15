package idletimer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

// The whole point of the cache: a round costs no apiserver reads.
func TestRound_ServesTheRecordFromTheCache(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.cacheRecord("member-1", h.clock.Now())
	h.fake.ClearActions()

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.fake.Actions(), "a cached record must not be read from the apiserver")
}

// A cache that lags reads as idle, and idleness is what triggers a suspend, so
// the decision is confirmed against the apiserver before it is acted on.
func TestRound_ConfirmsAStaleCachedIdleReadBeforeSuspending(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	require.NoError(t, h.recorder.client.create(t.Context(), "member-1", "uid-1", h.clock.Now()))

	// The cache still holds the value from before the last request.
	h.cacheRecord("member-1", h.clock.Now().Add(-2*threshold))
	h.clock.Advance(time.Second)

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept, "a stale cached read must not suspend a workload the apiserver says is active")
}

func TestRound_FallsBackToTheApiserverUntilTheCacheHasSynced(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.cacheRecord("member-1", h.clock.Now())
	h.synced.Store(false)
	h.fake.ClearActions()

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.NotEmpty(t, h.fake.Actions(), "before the cache has synced, absence from it is not absence")
}

// A suspend decision taken on a cached read is confirmed; one taken on a live
// read is not, so the confirm must not cost a second request.
func TestRound_SuspendsOnACachedReadTheApiserverAgreesWith(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, testMember))
	h.round()

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.flush()
	h.cacheLive(testMember)
	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Equal(t, []k8sapp.WorkloadRef{appRef}, h.source.slept)
}

// A redeploy leaves the old member's record in the cache. The candidate names
// the new one, and that is the only record that answers for it.
func TestRound_ACachedRecordIsKeyedByItsMember(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-2"))
	h.cacheRecord("member-1", h.clock.Now())
	h.fake.ClearActions()

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.NotEmpty(t, h.fake.Actions(), "another member's cached record must not answer for this one")

	// Once the watch catches up with the new member, the reads stop again.
	h.cacheRecord("member-2", h.clock.Now())
	h.fake.ClearActions()

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.fake.Actions())
}

// The cache lags, and a lagging copy can miss an entry declared since. The
// confirm read decides on the live entries.
func TestRound_TheConfirmReadAppliesTheLiveMargin(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, testMember))
	h.round()

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.flush()
	h.cacheLive(testMember)

	// The other replica declares a longer heartbeat after the cache was filled.
	require.NoError(t, h.recorder.client.write(t.Context(), testMember, "other", time.Time{}, entry{aliveAt: h.clock.Now(), heartbeat: maxHeartbeatInterval}))

	h.suspender.suspendIdleWorkloads(t.Context())

	assert.Empty(t, h.source.slept, "the live record says the other replica may be up to 5m behind")
}
