package k8sapp_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

const prodUpstreamURL = "http://prod.keboola.svc.cluster.local:8888"

// resolveHost returns the resolved workload, zero when no Sandbox owns the hostname.
func resolveHost(w *k8sapp.StateWatcher, ctx context.Context, host string) k8sapp.WorkloadRef {
	ref, _ := w.ResolveWorkloadForHost(ctx, host)
	return ref
}

// sandboxNameFor returns the Sandbox that owns the hostname, "" when none does.
func sandboxNameFor(w *k8sapp.StateWatcher, ctx context.Context, host string) string {
	ref, _ := w.ResolveWorkloadForHost(ctx, host)
	return ref.SandboxName
}

func newSandboxObject(k8sName, appID string, state k8sapp.AppActualState, publicURL, upstreamURL string) *unstructured.Unstructured {
	appsProxy := map[string]any{}
	if publicURL != "" {
		appsProxy["publicUrl"] = publicURL
	}
	if upstreamURL != "" {
		appsProxy["upstreamUrl"] = upstreamURL
	}
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + k8sapp.SandboxVersion,
			"kind":       "Sandbox",
			"metadata": map[string]any{
				"name":      k8sName,
				"namespace": testNamespace,
			},
			"spec": map[string]any{
				"appId": appID,
			},
			"status": map[string]any{
				"currentState": string(state),
				"appsProxy":    appsProxy,
			},
		},
	}
}

func createSandbox(t *testing.T, client *k8sfake.FakeDynamicClient, obj *unstructured.Unstructured) {
	t.Helper()
	_, err := client.Resource(k8sapp.SandboxGVR()).Namespace(testNamespace).Create(t.Context(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
}

func TestStateWatcher_ResolveWorkloadForHost_SandboxOwnsExactHostname(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject(
		"draft-abc", "app-123", k8sapp.AppActualStateRunning,
		"https://draft-abc.hub.example.com", "http://draft-abc.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	assert.Eventually(t, func() bool {
		ref := resolveHost(watcher, t.Context(), "draft-abc.hub.example.com")
		return ref.SandboxName == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)

	ref := resolveHost(watcher, t.Context(), "draft-abc.hub.example.com")
	info, ok := watcher.GetState(t.Context(), ref)
	require.True(t, ok)
	require.NotNil(t, info.UpstreamTarget)
	assert.Equal(t, "http://draft-abc.keboola.svc.cluster.local:8888", info.UpstreamTarget.String())
	assert.Equal(t, k8sapp.AppActualStateRunning, info.ActualState)
}

// A Sandbox with no publicUrl owns no route: the operator publishes the field
// only for a workload that owns its hostname.
func TestStateWatcher_ResolveWorkloadForHost_SandboxWithoutPublicURLIsNotRouted(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject(
		"member-1", "app-123", k8sapp.AppActualStateRunning, "", "http://member-1.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	ref := resolveHost(watcher, t.Context(), "member-1.hub.example.com")
	assert.Empty(t, ref.SandboxName)
}

// A Sandbox is reached only by an exact match on the hostname it published, and
// that match is authoritative. Draft hostnames are allocated by the platform, so
// one cannot collide with a hostname an App answers on.
func TestStateWatcher_ResolveWorkloadForHost_ExactMatchWinsOverTheApp(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	appObj := newAppObjectWithPublicURL("42", "https://myslug-42.hub.example.com")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"member-2", "42", k8sapp.AppActualStateRunning,
		"https://myslug-42.hub.example.com", "http://member-2.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	require.Eventually(t, func() bool {
		ref, ok := watcher.ResolveWorkloadForHost(t.Context(), "myslug-42.hub.example.com")
		return ok && ref.SandboxName == "member-2"
	}, 5*time.Second, 50*time.Millisecond)
}

func TestStateWatcher_ResolveWorkloadForHost_HostnameReleasedOnPublicURLChange(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	obj := newSandboxObject("draft-abc", "app-123", k8sapp.AppActualStateRunning, "https://draft-abc.hub.example.com", "http://a:8888")
	createSandbox(t, fakeClient, obj)

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-abc.hub.example.com") == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)

	updated := newSandboxObject("draft-abc", "app-123", k8sapp.AppActualStateRunning, "https://draft-xyz.hub.example.com", "http://a:8888")
	_, err := fakeClient.Resource(k8sapp.SandboxGVR()).Namespace(testNamespace).Update(t.Context(), updated, metav1.UpdateOptions{})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-xyz.hub.example.com") == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)

	assert.Empty(t, sandboxNameFor(watcher, t.Context(), "draft-abc.hub.example.com"))
}

func TestStateWatcher_ResolveWorkloadForHost_HostnameReleasedOnDelete(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject("draft-abc", "app-123", k8sapp.AppActualStateRunning, "https://draft-abc.hub.example.com", "http://a:8888"))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-abc.hub.example.com") == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, fakeClient.Resource(k8sapp.SandboxGVR()).Namespace(testNamespace).Delete(t.Context(), "draft-abc", metav1.DeleteOptions{}))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-abc.hub.example.com") == ""
	}, 5*time.Second, 50*time.Millisecond)
}

// A hostname owned by a Sandbox wakes that Sandbox CR, not the App.
func TestStateWatcher_Wakeup_PatchesSandboxCRD(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject("draft-abc", "app-123", k8sapp.AppActualStateStopped, "https://draft-abc.hub.example.com", "http://a:8888"))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	var ref k8sapp.WorkloadRef
	assert.Eventually(t, func() bool {
		ref = resolveHost(watcher, t.Context(), "draft-abc.hub.example.com")
		return ref.SandboxName == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, watcher.Wakeup(t.Context(), ref))

	got, err := fakeClient.Resource(k8sapp.SandboxGVR()).Namespace(testNamespace).Get(t.Context(), "draft-abc", metav1.GetOptions{})
	require.NoError(t, err)
	state, found, err := unstructured.NestedString(got.Object, "spec", "state")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, string(k8sapp.AppActualStateRunning), state)
}

func TestStateWatcher_ResolveWorkloadForHost_HostnameNormalisation(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject("draft-abc", "app-123", k8sapp.AppActualStateRunning, "https://Draft-ABC.hub.example.com", "http://a:8888"))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-abc.hub.example.com:8443") == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)
}

// newAppObjectWithPublicURL creates an App CRD object publishing both proxy URLs.
func newAppObjectWithPublicURL(appID, publicURL string) *unstructured.Unstructured {
	obj := newAppObject("prod-app", appID, k8sapp.AppActualStateRunning)
	obj.Object["status"].(map[string]any)["appsProxy"] = map[string]any{
		"publicUrl":   publicURL,
		"upstreamUrl": prodUpstreamURL,
	}
	return obj
}

// A draft owns a hostname of its own under the same appId as its owning App.
// The App is reached through hostname normalisation and keeps its own route, so
// both are routable at the same time.
func TestStateWatcher_ResolveWorkloadForHost_DraftAndAppCoexist(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	appObj := newAppObjectWithPublicURL("123", "https://public-123.hub.example.com")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"draft-abc", "123", k8sapp.AppActualStateRunning,
		"https://draft-abc-123.hub.example.com", "http://draft-abc.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-abc-123.hub.example.com") == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)

	draftInfo, ok := watcher.GetState(t.Context(), resolveHost(watcher, t.Context(), "draft-abc-123.hub.example.com"))
	require.True(t, ok)
	require.NotNil(t, draftInfo.UpstreamTarget)
	assert.Equal(t, "http://draft-abc.keboola.svc.cluster.local:8888", draftInfo.UpstreamTarget.String())

	// The App's own hostname is untouched: no Sandbox owns it, so the caller
	// falls through to hostname normalisation.
	_, claimed := watcher.ResolveWorkloadForHost(t.Context(), "public-123.hub.example.com")
	assert.False(t, claimed)
	appInfo, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
	require.True(t, ok)
	require.NotNil(t, appInfo.UpstreamTarget)
	assert.Equal(t, "http://prod.keboola.svc.cluster.local:8888", appInfo.UpstreamTarget.String())
}

// The hostname the operator will publish for a draft carries no appId at all.
// The appId comes from the Sandbox's own spec.
func TestStateWatcher_ResolveWorkloadForHost_AppIDComesFromSandboxSpec(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	appObj := newAppObjectWithPublicURL("123", "https://public-123.hub.example.com")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"draft-9f3c", "123", k8sapp.AppActualStateRunning,
		"https://draft-9f3c.hub.example.com", "http://draft-9f3c.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	var ref k8sapp.WorkloadRef
	var claimed bool
	require.Eventually(t, func() bool {
		ref, claimed = watcher.ResolveWorkloadForHost(t.Context(), "draft-9f3c.hub.example.com")
		return claimed
	}, 5*time.Second, 50*time.Millisecond)

	assert.Equal(t, k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-9f3c"}, ref)
}

// An App that has never been promoted publishes no hostname of its own. Its
// drafts were once refused because of that; they must route.
func TestStateWatcher_ResolveWorkloadForHost_NeverPublishedAppDoesNotBlockUnrelatedDraft(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	appObj := newAppObject("app-7327412", "7327412", k8sapp.AppActualStateRunning)
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"app-7327412-dft-01a0aa03", "7327412", k8sapp.AppActualStateRunning,
		"https://draft-01a0aa03-4886-74b4-87c2-8518d0ebf7f1.hub.example.com",
		"http://app-sb-app-7327412-dft-01a0aa03.sandbox.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	require.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "7327412"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	ref, claimed := watcher.ResolveWorkloadForHost(t.Context(), "draft-01a0aa03-4886-74b4-87c2-8518d0ebf7f1.hub.example.com")
	assert.True(t, claimed, "the draft owns this hostname; the App publishes none and would never publish this one")
	assert.Equal(t, k8sapp.WorkloadRef{AppID: "7327412", SandboxName: "app-7327412-dft-01a0aa03"}, ref)
}

// A wake for a workload that is not in the cache patches nothing. Returning nil
// there is indistinguishable from a successful wake, so a draft that can never
// start produces no signal anywhere.
func TestStateWatcher_Wakeup_ErrorsWhenWorkloadUnknown(t *testing.T) {
	t.Parallel()

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), newFakeClient(), testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	err := watcher.Wakeup(t.Context(), k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-gone"})
	require.Error(t, err, "a wake that patches nothing must not look like a successful wake")
	assert.Contains(t, err.Error(), "draft-gone")
}

// Caches keyed by workload cannot shed an entry unless the watcher says the
// workload is gone.
func TestStateWatcher_OnWorkloadRemoved(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject(
		"draft-abc", "123", k8sapp.AppActualStateRunning,
		"https://draft-abc.hub.example.com", "http://draft-abc.svc:8888",
	))
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), newAppObjectWithPublicURL("456", "https://public-456.hub.example.com"), metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	var mu sync.Mutex
	var removed []k8sapp.WorkloadRef
	watcher.OnWorkloadRemoved(func(ref k8sapp.WorkloadRef) {
		mu.Lock()
		defer mu.Unlock()
		removed = append(removed, ref)
	})

	require.True(t, watcher.WaitForCacheSync(t.Context()))
	require.Eventually(t, func() bool {
		_, sandbox := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"})
		_, app := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "456"})
		return sandbox && app
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, fakeClient.Resource(k8sapp.SandboxGVR()).Namespace(testNamespace).Delete(t.Context(), "draft-abc", metav1.DeleteOptions{}))
	require.NoError(t, fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Delete(t.Context(), "prod-app", metav1.DeleteOptions{}))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(removed) == 2
	}, 5*time.Second, 50*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, removed, k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"})
	assert.Contains(t, removed, k8sapp.WorkloadRef{AppID: "456"})
}
