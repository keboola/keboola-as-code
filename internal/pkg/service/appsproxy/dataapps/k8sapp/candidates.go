package k8sapp

import (
	"sort"

	"k8s.io/client-go/tools/cache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

// HasSynced reports whether both informers have completed their initial list.
// Unlike WaitForCacheSync it does not block, so a periodic caller can skip a
// round rather than act on a cache that is still filling.
func (w *StateWatcher) HasSynced() bool {
	return synced(w.hasSynced) && synced(w.sandboxesHaveSynced)
}

func synced(fn cache.InformerSynced) bool {
	return fn != nil && fn()
}

// ScanForSuspend returns the Running workloads the idle-suspend loop may act
// on, as the cache holds them at this moment. It walks and sorts, so a caller
// takes it once per round rather than per workload.
//
// A route is an App, or a Sandbox that published a hostname of its own. A
// member Sandbox publishes none and is therefore never returned on its own
// account: it reaches the caller as the SandboxName of the App that names it,
// so one workload stays one entry and production is suspended through the App.
func (w *StateWatcher) ScanForSuspend() SuspendScan {
	candidates, unresolved := w.collectCandidates()

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Ref.String() < candidates[j].Ref.String()
	})
	sort.Slice(unresolved, func(i, j int) bool { return unresolved[i] < unresolved[j] })

	return SuspendScan{Candidates: candidates, Unresolved: unresolved}
}

func (w *StateWatcher) collectCandidates() (candidates []SuspendCandidate, unresolved []api.AppID) {
	w.routeLock.RLock()
	defer w.routeLock.RUnlock()

	candidates = make([]SuspendCandidate, 0, len(w.apps)+len(w.sandboxes))

	for appID, e := range w.apps {
		if e.state != AppActualStateRunning {
			continue
		}
		member, ok := w.sandboxes[e.productionSandbox]
		if e.productionSandbox == "" || !ok {
			unresolved = append(unresolved, appID)
			continue
		}
		candidates = append(candidates, SuspendCandidate{
			Ref:         WorkloadRef{AppID: appID},
			SandboxName: e.productionSandbox,
			SandboxUID:  member.uid,
			Threshold:   member.autoSuspendAfter,
			LastStarted: member.lastStarted,
		})
	}

	for k8sName, e := range w.sandboxes {
		if e.state != AppActualStateRunning || e.host == "" {
			continue
		}
		candidates = append(candidates, SuspendCandidate{
			Ref:         WorkloadRef{AppID: e.appID, SandboxName: k8sName},
			SandboxName: k8sName,
			SandboxUID:  e.uid,
			Threshold:   e.autoSuspendAfter,
			LastStarted: e.lastStarted,
		})
	}

	return candidates, unresolved
}
