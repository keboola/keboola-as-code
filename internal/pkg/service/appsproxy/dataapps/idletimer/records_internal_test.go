package idletimer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

// The whole point of the cache: a round costs no apiserver reads.
func TestTick_ServesTheRecordFromTheCache(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.cacheRecord("member-1", h.clock.Now())
	h.fake.ClearActions()

	h.manager.tick(t.Context())

	assert.Empty(t, h.fake.Actions(), "a cached record must not be read from the apiserver")
}

// A cache that lags reads as idle, and idleness is what triggers a suspend, so
// the decision is confirmed against the apiserver before it is acted on.
func TestTick_ConfirmsAStaleCachedIdleReadBeforeSuspending(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	require.NoError(t, h.manager.client.create(t.Context(), "member-1", "uid-1", h.clock.Now()))

	// The cache still holds the value from before the last request.
	h.cacheRecord("member-1", h.clock.Now().Add(-2*threshold))
	h.clock.Advance(time.Second)

	h.manager.tick(t.Context())

	assert.Empty(t, h.source.slept, "a stale cached read must not suspend a workload the apiserver says is active")
}

func TestTick_FallsBackToTheApiserverUntilTheCacheHasSynced(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	h.cacheRecord("member-1", h.clock.Now())
	h.synced.Store(false)
	h.fake.ClearActions()

	h.manager.tick(t.Context())

	assert.NotEmpty(t, h.fake.Actions(), "before the cache has synced, absence from it is not absence")
}

// A suspend decision taken on a cached read is confirmed; one taken on a live
// read is not, so the confirm must not cost a second request.
func TestTick_SuspendsOnACachedReadTheApiserverAgreesWith(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-1"))
	require.NoError(t, h.manager.client.create(t.Context(), "member-1", "uid-1", h.clock.Now()))
	h.cacheRecord("member-1", h.clock.Now())

	h.clock.Advance(threshold + heartbeatInterval(threshold) + time.Second)
	h.manager.tick(t.Context())

	assert.Equal(t, []k8sapp.WorkloadRef{appRef}, h.source.slept)
}

// A redeploy leaves the old member's record in the cache. The candidate names
// the new one, and that is the only record that answers for it.
func TestTick_ACachedRecordIsKeyedByItsMember(t *testing.T) {
	t.Parallel()

	h := newHarness(t, candidate(appRef, "member-2"))
	h.cacheRecord("member-1", h.clock.Now())
	h.fake.ClearActions()

	h.manager.tick(t.Context())

	assert.NotEmpty(t, h.fake.Actions(), "another member's cached record must not answer for this one")

	// Once the watch catches up with the new member, the reads stop again.
	h.cacheRecord("member-2", h.clock.Now())
	h.fake.ClearActions()

	h.manager.tick(t.Context())

	assert.Empty(t, h.fake.Actions())
}
