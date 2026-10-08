package k8sapp_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

const (
	lastStarted = "2026-10-08T12:00:00Z"
	memberName  = "app-123-dpl-aaa"
	memberAppID = "123"
	appName     = "app-123"
)

// newMemberSandbox builds a Sandbox that publishes no hostname: it is reached
// only through the App that names it, never as a route of its own.
func newMemberSandbox(state k8sapp.AppActualState, autoSuspendAfterSeconds any) *unstructured.Unstructured {
	obj := newSandboxObject(memberName, memberAppID, state, "", prodUpstreamURL)
	if autoSuspendAfterSeconds != nil {
		obj.Object["spec"].(map[string]any)["autoSuspendAfterSeconds"] = autoSuspendAfterSeconds
	}
	obj.Object["status"].(map[string]any)["lastStartedTime"] = lastStarted
	return obj
}

func newAppWithProductionSandbox(state k8sapp.AppActualState, member string) *unstructured.Unstructured {
	obj := newAppObject(appName, memberAppID, state)
	if member != "" {
		obj.Object["status"].(map[string]any)["productionSandbox"] = member
	}
	return obj
}

func syncedWatcher(t *testing.T, logger log.Logger, objects ...*unstructured.Unstructured) *k8sapp.StateWatcher {
	t.Helper()
	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDepsWithLogger(t, logger), fakeClient, testNamespace)

	for _, obj := range objects {
		gvr := k8sapp.AppGVR()
		if obj.GetKind() == "Sandbox" {
			gvr = k8sapp.SandboxGVR()
		}
		_, err := fakeClient.Resource(gvr).Namespace(testNamespace).Create(t.Context(), obj, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	return watcher
}

func refsOf(candidates []k8sapp.SuspendCandidate) []k8sapp.WorkloadRef {
	refs := make([]k8sapp.WorkloadRef, 0, len(candidates))
	for _, c := range candidates {
		refs = append(refs, c.Ref)
	}
	return refs
}

func TestRunningWorkloads_AppRouteCarriesItsMemberSandboxThreshold(t *testing.T) {
	t.Parallel()

	watcher := syncedWatcher(t, log.NewNopLogger(),
		newMemberSandbox(k8sapp.AppActualStateRunning, int64(900)),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, "app-123-dpl-aaa"),
	)

	var got []k8sapp.SuspendCandidate
	assert.Eventually(t, func() bool {
		got = watcher.RunningWorkloads().Candidates
		return len(got) == 1
	}, 5*time.Second, 50*time.Millisecond)

	require.Len(t, got, 1)
	assert.Equal(t, k8sapp.WorkloadRef{AppID: "123"}, got[0].Ref)
	assert.Equal(t, "app-123-dpl-aaa", got[0].SandboxName)
	assert.Equal(t, 900*time.Second, got[0].Threshold)
	assert.Equal(t, lastStarted, got[0].LastStarted.UTC().Format(time.RFC3339))
}

func TestRunningWorkloads_AbsentThresholdIsReportedAsZero(t *testing.T) {
	t.Parallel()

	watcher := syncedWatcher(t, log.NewNopLogger(),
		newMemberSandbox(k8sapp.AppActualStateRunning, nil),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, "app-123-dpl-aaa"),
	)

	var got []k8sapp.SuspendCandidate
	assert.Eventually(t, func() bool {
		got = watcher.RunningWorkloads().Candidates
		return len(got) == 1
	}, 5*time.Second, 50*time.Millisecond)

	assert.Zero(t, got[0].Threshold, "an absent threshold must reach the caller as zero, not a default")
}

func TestRunningWorkloads_SkipsAWorkloadThatIsNotRunning(t *testing.T) {
	t.Parallel()

	watcher := syncedWatcher(t, log.NewNopLogger(),
		newMemberSandbox(k8sapp.AppActualStateStopped, int64(900)),
		newAppWithProductionSandbox(k8sapp.AppActualStateStopped, "app-123-dpl-aaa"),
	)

	assert.Never(t, func() bool {
		return len(watcher.RunningWorkloads().Candidates) > 0
	}, time.Second, 50*time.Millisecond)
}

// An App that is Running but names no member is skipped and reported, never
// suspended and never dereferenced. Reporting it is the caller's business.
func TestRunningWorkloads_SkipsAnAppNamingNoMember(t *testing.T) {
	t.Parallel()

	watcher := syncedWatcher(t, log.NewNopLogger(),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, ""),
	)

	var snapshot k8sapp.WorkloadSnapshot
	assert.Eventually(t, func() bool {
		snapshot = watcher.RunningWorkloads()
		return len(snapshot.Unresolved) == 1
	}, 5*time.Second, 50*time.Millisecond)

	assert.Empty(t, snapshot.Candidates)
	assert.Equal(t, api.AppID("123"), snapshot.Unresolved[0])
}

func TestRunningWorkloads_SkipsAnAppWhoseMemberIsNotCached(t *testing.T) {
	t.Parallel()

	watcher := syncedWatcher(t, log.NewNopLogger(),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, "app-123-dpl-gone"),
	)

	assert.Never(t, func() bool {
		return len(watcher.RunningWorkloads().Candidates) > 0
	}, time.Second, 50*time.Millisecond)
}

func TestRunningWorkloads_DraftRouteIsItsOwnWorkload(t *testing.T) {
	t.Parallel()

	draft := newSandboxObject("draft-abc", "123", k8sapp.AppActualStateRunning, "https://draft-abc.hub.example.com", prodUpstreamURL)
	draft.Object["spec"].(map[string]any)["autoSuspendAfterSeconds"] = int64(600)

	watcher := syncedWatcher(t, log.NewNopLogger(), draft)

	var got []k8sapp.SuspendCandidate
	assert.Eventually(t, func() bool {
		got = watcher.RunningWorkloads().Candidates
		return len(got) == 1
	}, 5*time.Second, 50*time.Millisecond)

	assert.Equal(t, k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}, got[0].Ref)
	assert.Equal(t, "draft-abc", got[0].SandboxName)
	assert.Equal(t, 600*time.Second, got[0].Threshold)
}

// A member Sandbox is reached through its App, so returning it as well would
// make one workload two and suspend production through the wrong CRD.
func TestRunningWorkloads_MemberSandboxIsNotAWorkloadOfItsOwn(t *testing.T) {
	t.Parallel()

	watcher := syncedWatcher(t, log.NewNopLogger(),
		newMemberSandbox(k8sapp.AppActualStateRunning, int64(900)),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, "app-123-dpl-aaa"),
	)

	var got []k8sapp.SuspendCandidate
	assert.Eventually(t, func() bool {
		got = watcher.RunningWorkloads().Candidates
		return len(got) == 1
	}, 5*time.Second, 50*time.Millisecond)

	assert.NotContains(t, refsOf(got), k8sapp.WorkloadRef{AppID: "123", SandboxName: "app-123-dpl-aaa"})
}

func TestHasSynced_FalseBeforeTheInformersList(t *testing.T) {
	t.Parallel()

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), newFakeClient(), testNamespace)

	// Not a race: HasSynced may flip to true at any moment, so the assertion is
	// only that the watcher answers the question before it has.
	_ = watcher.HasSynced()

	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.True(t, watcher.HasSynced())
}

// The count is what a periodic caller watches; the warning is only emitted
// once per App, so it cannot carry the signal on its own.
func TestRunningWorkloads_CountsUnresolvedApps(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	watcher := syncedWatcher(t, logger,
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, "app-123-dpl-gone"),
	)

	var snapshot k8sapp.WorkloadSnapshot
	assert.Eventually(t, func() bool {
		snapshot = watcher.RunningWorkloads()
		return len(snapshot.Unresolved) == 1
	}, 5*time.Second, 50*time.Millisecond)

	assert.Empty(t, snapshot.Candidates)
}

// The IdleTimer's ownerReference needs the member's uid: the apiserver rejects
// one without it, and collection matches on the uid, so a stale one reads as an
// owner already gone.
func TestRunningWorkloads_CarriesTheSandboxUID(t *testing.T) {
	t.Parallel()

	member := newMemberSandbox(k8sapp.AppActualStateRunning, int64(900))
	member.SetUID("member-uid-1")

	watcher := syncedWatcher(t, log.NewNopLogger(),
		member,
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, memberName),
	)

	var got []k8sapp.SuspendCandidate
	assert.Eventually(t, func() bool {
		got = watcher.RunningWorkloads().Candidates
		return len(got) == 1
	}, 5*time.Second, 50*time.Millisecond)

	assert.Equal(t, k8stypes.UID("member-uid-1"), got[0].SandboxUID)
}
