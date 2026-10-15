// Package idletimer suspends a data app workload that has stopped receiving
// requests, mirroring the wakeup the proxy already performs for a stopped one.
package idletimer

import "time"

const (
	tickInterval         = 15 * time.Second
	heartbeatsPerWindow  = 20
	maxHeartbeatInterval = 300 * time.Second

	// A threshold below this is raised, never treated as absent: absence means
	// never auto-suspend, which is a different decision.
	minThreshold = 60 * time.Second

	// overdueAfter is how many of its own heartbeats an entry may go unwritten
	// before it counts as a pod that is gone.
	overdueAfter = 3
)

// lastSeen takes the latest of the three activity signals. A restart counts:
// a workload whose record outlived a suspend would otherwise be judged against
// a timestamp from before it came back.
func lastSeen(memory, recorded, started time.Time) time.Time {
	latest := memory
	if recorded.After(latest) {
		latest = recorded
	}
	if started.After(latest) {
		latest = started
	}
	return latest
}

// shouldSuspend adds a margin to the threshold: the longest a shared record may
// trail the last request. That is the threshold's own heartbeat, or any longer
// one a replica still writing declares — a record written under an old, longer
// heartbeat trails by that one until the trail is written.
//
// Both terms use the clamped threshold. Clamping only one of them suspends a
// sub-minimum workload early.
func shouldSuspend(idleFor, threshold, declared time.Duration) bool {
	effective := effectiveThreshold(threshold)
	return idleFor > effective+max(heartbeatInterval(effective), declared)
}

func heartbeatInterval(threshold time.Duration) time.Duration {
	if interval := effectiveThreshold(threshold) / heartbeatsPerWindow; interval < maxHeartbeatInterval {
		return interval
	}
	return maxHeartbeatInterval
}

func effectiveThreshold(threshold time.Duration) time.Duration {
	if threshold < minThreshold {
		return minThreshold
	}
	return threshold
}

func (e entry) overdue(now time.Time) bool {
	return now.Sub(e.aliveAt) > overdueAfter*e.heartbeat
}

// margin is the longest heartbeat declared by a replica still writing. It
// reports false when this replica's own entry is missing or overdue: its own
// writes are not landing, the other replica's may not be either, and the record
// cannot be read as idleness.
func (r record) margin(self string, now time.Time) (time.Duration, bool) {
	own, found := r.entries[self]
	if !found || own.overdue(now) {
		return 0, false
	}

	var margin time.Duration
	for _, e := range r.entries {
		if !e.overdue(now) {
			margin = max(margin, e.heartbeat)
		}
	}
	return margin, true
}
