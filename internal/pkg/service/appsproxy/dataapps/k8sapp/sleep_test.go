package k8sapp_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

func TestStateWatcher_Sleep_PatchesTheAppCRForAnAppRoute(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).
		Create(t.Context(), newAppObject("app-123", "123", k8sapp.AppActualStateRunning), metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	fakeClient.ClearActions()
	suspended, err := watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123"}, "")
	require.NoError(t, err)
	assert.True(t, suspended)

	patch := lastPatch(t, fakeClient)
	assert.Equal(t, k8sapp.Resource, patch.GetResource().Resource)
	assert.Equal(t, "app-123", patch.GetName())
	assert.Contains(t, string(patch.GetPatch()), `"state":"Stopped"`)
}

func TestStateWatcher_Sleep_PatchesTheSandboxCRForADraftRoute(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	_, err := fakeClient.Resource(k8sapp.SandboxGVR()).Namespace(testNamespace).
		Create(t.Context(), newSandboxObject("draft-abc", "123", k8sapp.AppActualStateRunning, "https://draft-abc.hub.example.com", prodUpstreamURL), metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	fakeClient.ClearActions()
	suspended, err := watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}, "draft-abc")
	require.NoError(t, err)
	assert.True(t, suspended)

	patch := lastPatch(t, fakeClient)
	assert.Equal(t, k8sapp.SandboxResource, patch.GetResource().Resource)
	assert.Equal(t, "draft-abc", patch.GetName())
}

// The precondition is what makes the suspend act on the object it just read.
func TestStateWatcher_Sleep_SendsAResourceVersionPrecondition(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).
		Create(t.Context(), newAppObject("app-123", "123", k8sapp.AppActualStateRunning), metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	fakeClient.ClearActions()
	_, err = watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123"}, "")
	require.NoError(t, err)

	assert.Contains(t, string(lastPatch(t, fakeClient).GetPatch()), `"resourceVersion"`)
}

// The suspend patch must never write the wake gate: a workload stopped with
// autoRestartEnabled=false cannot be woken by the next request.
func TestStateWatcher_Sleep_NeverWritesAutoRestartEnabled(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).
		Create(t.Context(), newAppObject("app-123", "123", k8sapp.AppActualStateRunning), metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	fakeClient.ClearActions()
	_, err = watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123"}, "")
	require.NoError(t, err)

	assert.NotContains(t, string(lastPatch(t, fakeClient).GetPatch()), "autoRestartEnabled")
}

// A workload that stopped between the tick's decision and the patch must be
// left alone, so a suspend never lands on something already moving.
func TestStateWatcher_Sleep_SkipsAWorkloadNoLongerRunning(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).
		Create(t.Context(), newAppObject("app-123", "123", k8sapp.AppActualStateStopping), metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	fakeClient.ClearActions()
	suspended, err := watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123"}, "")
	require.NoError(t, err)
	assert.False(t, suspended)

	for _, action := range fakeClient.Actions() {
		assert.NotEqual(t, "patch", action.GetVerb(), "a workload that is not Running must not be patched")
	}
}

func TestStateWatcher_Sleep_ReportsUnknownWorkload(t *testing.T) {
	t.Parallel()

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), newFakeClient(), testNamespace)

	_, err := watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "nope"}, "")
	require.Error(t, err)
}

func lastPatch(t *testing.T, fakeClient *k8sfake.FakeDynamicClient) k8stesting.PatchAction {
	t.Helper()
	for _, action := range fakeClient.Actions() {
		if patch, ok := action.(k8stesting.PatchAction); ok {
			return patch
		}
	}
	t.Fatal("no patch action was recorded")
	return nil
}

// The tick judges one member idle; by the time the patch goes out the App may
// have been redeployed onto another. Stopping the App then stops the deployment
// that just started. The resourceVersion precondition cannot catch this: it
// guards the gap between the read and the patch, not between the decision and
// the read.
func TestStateWatcher_Sleep_SkipsWhenTheMemberChanged(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	app := newAppObject("app-123", "123", k8sapp.AppActualStateRunning)
	app.Object["status"].(map[string]any)["productionSandbox"] = "member-new"
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).
		Create(t.Context(), app, metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	fakeClient.ClearActions()
	suspended, err := watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123"}, "member-old")
	require.NoError(t, err)
	assert.False(t, suspended)

	for _, action := range fakeClient.Actions() {
		assert.NotEqual(t, "patch", action.GetVerb(), "a redeployed App must not be stopped on the old member's idleness")
	}
}

func TestStateWatcher_Sleep_PatchesWhenTheMemberStillMatches(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	app := newAppObject("app-123", "123", k8sapp.AppActualStateRunning)
	app.Object["status"].(map[string]any)["productionSandbox"] = "member-1"
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).
		Create(t.Context(), app, metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	suspended, err := watcher.Sleep(t.Context(), k8sapp.WorkloadRef{AppID: "123"}, "member-1")
	require.NoError(t, err)
	assert.True(t, suspended)
}
