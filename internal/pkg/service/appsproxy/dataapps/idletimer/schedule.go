// Package idletimer suspends a data app workload that has stopped receiving
// requests, mirroring the wakeup the proxy already performs for a stopped one.
package idletimer

import "time"

const (
	// tickInterval is how often the suspend loop walks the cached workloads.
	tickInterval = 15 * time.Second

	// heartbeatsPerWindow fixes how far the shared record can lag reality: one
	// idle window divided this many ways. The suspend margin is one such
	// interval, so this also bounds how late a suspend can land.
	heartbeatsPerWindow = 20

	// maxHeartbeatInterval keeps a very long threshold from stretching the
	// margin to hours.
	maxHeartbeatInterval = 300 * time.Second

	// minThreshold clamps a threshold the operator would have rejected anyway.
	// A threshold below this is raised, never treated as absent: absence means
	// never auto-suspend, which is a different decision.
	minThreshold = 60 * time.Second
)

// lastSeen takes the latest of the three activity signals. A restart counts as
// activity: a workload whose record outlived a suspend would otherwise be
// judged against a timestamp from before it came back.
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

// shouldSuspend adds one heartbeat interval to the threshold before suspending.
// The shared record trails the real last request by at most that interval, so
// the margin is what makes suspending before the threshold impossible.
func shouldSuspend(idleFor, threshold time.Duration) bool {
	effective := effectiveThreshold(threshold)
	return idleFor > effective+heartbeatInterval(effective)
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
