package k8sapp

import (
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

// ScanForSleepCandidates returns the Running workloads the idle-suspend loop
// may act on, as the cache holds them at this moment. It walks and sorts, so a
// caller takes it once per round rather than per workload.
//
// A route is an App, or a Sandbox that published a hostname of its own. A
// member Sandbox publishes none and is therefore never returned on its own
// account: it reaches the caller as the SandboxName of the App that names it,
// so one workload stays one entry and production is slept through the App.
func (w *StateWatcher) ScanForSleepCandidates() SleepScan {
	candidates, unresolved := w.collectCandidates()

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Ref.String() < candidates[j].Ref.String()
	})
	sort.Slice(unresolved, func(i, j int) bool { return unresolved[i] < unresolved[j] })

	return SleepScan{Candidates: candidates, Unresolved: unresolved}
}

func (w *StateWatcher) collectCandidates() (candidates []SleepCandidate, unresolved []api.AppID) {
	w.routeLock.RLock()
	defer w.routeLock.RUnlock()

	candidates = make([]SleepCandidate, 0, len(w.apps)+len(w.sandboxes))

	for appID, e := range w.apps {
		if e.state != AppActualStateRunning || e.static {
			continue
		}
		member, ok := w.productionMemberLocked(e)
		if !ok {
			unresolved = append(unresolved, appID)
			continue
		}
		// The operator stamps the App when its route moves to another member,
		// which may have started long before and kept an idle timer from then.
		lastStarted := member.lastStarted
		if e.lastStarted.After(lastStarted) {
			lastStarted = e.lastStarted
		}
		candidates = append(candidates, SleepCandidate{
			Ref:         WorkloadRef{AppID: appID},
			SandboxName: e.productionSandbox,
			SandboxUID:  member.uid,
			Threshold:   member.autoSuspendAfter,
			LastStarted: lastStarted,
		})
	}

	for k8sName, e := range w.sandboxes {
		if e.state != AppActualStateRunning || e.host == "" {
			continue
		}
		candidates = append(candidates, SleepCandidate{
			Ref:         WorkloadRef{AppID: e.appID, SandboxName: k8sName},
			SandboxName: k8sName,
			SandboxUID:  e.uid,
			Threshold:   e.autoSuspendAfter,
			LastStarted: e.lastStarted,
		})
	}

	return candidates, unresolved
}
