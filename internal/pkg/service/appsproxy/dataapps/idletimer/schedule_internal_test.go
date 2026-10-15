package idletimer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeartbeatInterval(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		threshold time.Duration
		want      time.Duration
	}{
		{"common 15m threshold", 900 * time.Second, 45 * time.Second},
		{"1h threshold", 3600 * time.Second, 180 * time.Second},
		{"24h threshold hits the cap", 86400 * time.Second, 300 * time.Second},
		{"exactly at the cap", 6000 * time.Second, 300 * time.Second},
		{"minimum threshold", 60 * time.Second, 3 * time.Second},
		{"below the minimum is clamped up, not refused", 30 * time.Second, 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, heartbeatInterval(tc.threshold))
		})
	}
}

func TestLastSeen(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	memory := base.Add(1 * time.Minute)
	recorded := base.Add(2 * time.Minute)
	started := base.Add(3 * time.Minute)

	for _, tc := range []struct {
		name                      string
		memory, recorded, started time.Time
		want                      time.Time
	}{
		{"in-memory last request wins", started, recorded, memory, started},
		{"the record wins", memory, started, recorded, started},
		{"the restart time wins", memory, recorded, started, started},
		{"a zero input never wins", time.Time{}, recorded, time.Time{}, recorded},
		{"all zero yields zero", time.Time{}, time.Time{}, time.Time{}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, lastSeen(tc.memory, tc.recorded, tc.started))
		})
	}
}

func TestShouldSuspend(t *testing.T) {
	t.Parallel()

	const threshold = 900 * time.Second
	margin := threshold + heartbeatInterval(threshold)

	for _, tc := range []struct {
		name    string
		idleFor time.Duration
		want    bool
	}{
		{"active", 0, false},
		{"idle for just under the threshold", threshold - time.Second, false},
		{"idle for exactly the threshold is never suspended", threshold, false},
		{"idle for the threshold plus part of the margin", threshold + 20*time.Second, false},
		{"idle for exactly the threshold plus the margin", margin, false},
		{"idle past the margin", margin + time.Nanosecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, shouldSuspend(tc.idleFor, threshold, 0))
		})
	}
}

func TestShouldSuspend_ClampedThresholdWidensTheMargin(t *testing.T) {
	t.Parallel()

	// A sub-minimum threshold is raised to minThreshold, so a workload idle for
	// its literal threshold must still not be suspended.
	assert.False(t, shouldSuspend(30*time.Second, 30*time.Second, 0))
	assert.False(t, shouldSuspend(minThreshold, 30*time.Second, 0))
	assert.True(t, shouldSuspend(minThreshold+heartbeatInterval(minThreshold)+time.Nanosecond, 30*time.Second, 0))
}

// A replica still writing under a longer heartbeat may leave the record that
// much behind, whatever the current threshold's own heartbeat is.
func TestShouldSuspend_ALongerDeclaredHeartbeatWidensTheMargin(t *testing.T) {
	t.Parallel()

	const declared = 3 * time.Minute

	assert.False(t, shouldSuspend(minThreshold+declared, minThreshold, declared))
	assert.True(t, shouldSuspend(minThreshold+declared+time.Nanosecond, minThreshold, declared))
}

func TestMargin(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	fresh := func(heartbeat time.Duration) entry { return entry{aliveAt: now, heartbeat: heartbeat} }
	overdue := func(heartbeat time.Duration) entry {
		return entry{aliveAt: now.Add(-overdueAfter*heartbeat - time.Nanosecond), heartbeat: heartbeat}
	}

	for _, tc := range []struct {
		name       string
		entries    map[string]entry
		wantMargin time.Duration
		wantOK     bool
	}{
		{"no own entry", map[string]entry{"other": fresh(time.Minute)}, 0, false},
		{"own entry overdue", map[string]entry{testSelf: overdue(time.Minute)}, 0, false},
		{"the longest live heartbeat", map[string]entry{testSelf: fresh(time.Minute), "other": fresh(3 * time.Minute)}, 3 * time.Minute, true},
		{"an overdue entry does not count", map[string]entry{testSelf: fresh(time.Minute), "gone": overdue(5 * time.Minute)}, time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			margin, ok := record{entries: tc.entries}.margin(testSelf, now)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantMargin, margin)
		})
	}
}
