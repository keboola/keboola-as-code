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

// shouldSuspend adds one heartbeat interval to the threshold. The shared record
// trails the real last request by at most that interval, so the margin is what
// makes suspending before the threshold impossible.
//
// Both terms use the clamped threshold. Clamping only one of them suspends a
// sub-minimum workload early.
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
