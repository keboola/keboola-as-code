package k8sapp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

const prodUpstreamURL = "http://prod.keboola.svc.cluster.local:8888"

// resolveHost returns the resolved workload, zero when the App keeps the route.
func resolveHost(w *k8sapp.StateWatcher, ctx context.Context, host string) k8sapp.WorkloadRef {
	ref, _ := w.ResolveHost(ctx, host)
	return ref
}

// sandboxNameFor returns the Sandbox that owns the hostname, "" when the App does.
func sandboxNameFor(w *k8sapp.StateWatcher, ctx context.Context, host string) string {
	ref, _ := w.ResolveHost(ctx, host)
	return ref.SandboxName
}

// newSandboxObject creates an unstructured Sandbox CRD object.
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

func TestStateWatcher_ResolveHost_SandboxOwnsExactHostname(t *testing.T) {
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
func TestStateWatcher_ResolveHost_SandboxWithoutPublicURLIsNotRouted(t *testing.T) {
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

// The App owns the whole normalised hostname namespace. A member Sandbox that
// publishes production's own hostname must not divert production traffic.
func TestStateWatcher_ResolveHost_AppWinsHostnameTie(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	appObj := newAppObjectWithPublicURL("42", "https://myslug-42.hub.example.com")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"member-2", "42", k8sapp.AppActualStateStopped,
		"https://myslug-42.hub.example.com", "http://member-2.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	// Wait until both objects are in the cache, so the tie is actually reachable.
	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "42"})
		if !ok {
			return false
		}
		_, ok = watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "42", SandboxName: "member-2"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	_, claimed := watcher.ResolveHost(t.Context(), "myslug-42.hub.example.com")
	assert.False(t, claimed)

	info, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "42"})
	require.True(t, ok)
	require.NotNil(t, info.UpstreamTarget)
	assert.Equal(t, "http://prod.keboola.svc.cluster.local:8888", info.UpstreamTarget.String())
}

func TestStateWatcher_ResolveHost_HostnameReleasedOnPublicURLChange(t *testing.T) {
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

func TestStateWatcher_ResolveHost_HostnameReleasedOnDelete(t *testing.T) {
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

// Hostname matching ignores the request port and letter case.
func TestStateWatcher_ResolveHost_HostnameNormalisation(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	createSandbox(t, fakeClient, newSandboxObject("draft-abc", "app-123", k8sapp.AppActualStateRunning, "https://Draft-ABC.hub.example.com", "http://a:8888"))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-abc.hub.example.com:8443") == "draft-abc"
	}, 5*time.Second, 50*time.Millisecond)
}

// withProxyIngress marks the workload as having apps-proxy ingress enabled, which
// is what makes the operator publish a hostname for it.
func withProxyIngress(obj *unstructured.Unstructured) *unstructured.Unstructured {
	obj.Object["spec"].(map[string]any)["features"] = map[string]any{
		"appsProxyIngress": map[string]any{"targetPort": int64(8888)},
	}
	return obj
}

// newAppObjectWithPublicURL creates an App CRD object publishing both proxy URLs.
func newAppObjectWithPublicURL(appID, publicURL string) *unstructured.Unstructured {
	obj := withProxyIngress(newAppObject("prod-app", appID, k8sapp.AppActualStateRunning))
	obj.Object["status"].(map[string]any)["appsProxy"] = map[string]any{
		"publicUrl":   publicURL,
		"upstreamUrl": prodUpstreamURL,
	}
	return obj
}

// A draft owns a hostname of its own under the same appId as its owning App.
// The App is reached through hostname normalisation and keeps its own route, so
// both are routable at the same time.
func TestStateWatcher_ResolveHost_DraftAndAppCoexist(t *testing.T) {
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
	_, claimed := watcher.ResolveHost(t.Context(), "public-123.hub.example.com")
	assert.False(t, claimed)
	appInfo, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
	require.True(t, ok)
	require.NotNil(t, appInfo.UpstreamTarget)
	assert.Equal(t, "http://prod.keboola.svc.cluster.local:8888", appInfo.UpstreamTarget.String())
}

// A member that inherited the App's slug claims exactly the hostname the App is
// about to publish. While the App's status has not caught up, the App still
// wins that hostname, or the member takes production traffic.
func TestStateWatcher_ResolveHost_AppWithoutPublicURLKeepsItsOwnHostname(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	// spec.features.appsProxyIngress set with the slug, status.appsProxy not yet written.
	appObj := withProxyIngressSlug(newAppObject("prod-app", "123", k8sapp.AppActualStateRunning), "prod")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"member-2", "123", k8sapp.AppActualStateRunning,
		"https://prod-123.hub.example.com", "http://member-2.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	require.Eventually(t, func() bool {
		_, sandboxCached := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123", SandboxName: "member-2"})
		_, appCached := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "123"})
		return sandboxCached && appCached
	}, 5*time.Second, 50*time.Millisecond)

	_, claimed := watcher.ResolveHost(t.Context(), "prod-123.hub.example.com")
	assert.False(t, claimed, "the App is about to publish this hostname and must keep it")
}

// The hostname tie is hit on every production request while every deployment
// member publishes production's own hostname, so it must be logged when the
// claim is registered, not per request.
func TestStateWatcher_ResolveHost_TieWarnsOncePerClaim(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	logger := log.NewDebugLogger()

	appObj := newAppObjectWithPublicURL("42", "https://myslug-42.hub.example.com")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"member-2", "42", k8sapp.AppActualStateStopped,
		"https://myslug-42.hub.example.com", "http://member-2.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDepsWithLogger(t, logger), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	// The tie is reported once, while the CRD event is processed.
	require.Eventually(t, func() bool {
		return strings.Contains(logger.WarnMessages(), "the App keeps the route")
	}, 5*time.Second, 50*time.Millisecond)
	assert.Equal(t, 1, strings.Count(logger.WarnMessages(), "the App keeps the route"))

	// Requests must not add to that: today every deployment member publishes
	// production's own hostname, so the tie is hit on every production request.
	logger.Truncate()
	for range 3 {
		assert.Empty(t, sandboxNameFor(watcher, t.Context(), "myslug-42.hub.example.com"))
	}
	assert.NotContains(t, logger.WarnMessages(), "the App keeps the route")
}

// The hostname the operator will publish for a draft carries no appId at all.
// The appId comes from the Sandbox's own spec.
func TestStateWatcher_ResolveHost_AppIDComesFromSandboxSpec(t *testing.T) {
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
		ref, claimed = watcher.ResolveHost(t.Context(), "draft-9f3c.hub.example.com")
		return claimed
	}, 5*time.Second, 50*time.Millisecond)

	assert.Equal(t, k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-9f3c"}, ref)
}

// An App with apps-proxy ingress disabled never publishes a hostname, so the
// backfill guard must not treat it as owning one — that would block its drafts
// permanently rather than for a startup window.
func TestStateWatcher_ResolveHost_IngressDisabledAppDoesNotBlockDraft(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	// No spec.features.appsProxyIngress, and therefore no status publicUrl.
	appObj := newAppObjectWithUpstreamURL("prod-app", "123", k8sapp.AppActualStateRunning, "http://prod.keboola.svc.cluster.local:8888")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(t.Context(), appObj, metav1.CreateOptions{})
	require.NoError(t, err)

	createSandbox(t, fakeClient, newSandboxObject(
		"draft-9f3c", "123", k8sapp.AppActualStateRunning,
		"https://draft-9f3c.hub.example.com", "http://draft-9f3c.keboola.svc.cluster.local:8888",
	))

	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))

	require.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "draft-9f3c.hub.example.com") == "draft-9f3c"
	}, 5*time.Second, 50*time.Millisecond)
}

// A deleted App must release its hostname, or its Sandboxes stay unroutable.
func TestStateWatcher_ResolveHost_AppHostReleasedOnDelete(t *testing.T) {
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
		_, ok := watcher.GetState(t.Context(), k8sapp.WorkloadRef{AppID: "42"})
		return ok
	}, 5*time.Second, 50*time.Millisecond)
	assert.Empty(t, sandboxNameFor(watcher, t.Context(), "myslug-42.hub.example.com"))

	require.NoError(t, fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Delete(t.Context(), "prod-app", metav1.DeleteOptions{}))

	assert.Eventually(t, func() bool {
		return sandboxNameFor(watcher, t.Context(), "myslug-42.hub.example.com") == "member-2"
	}, 5*time.Second, 50*time.Millisecond)
}

// withProxyIngressSlug marks apps-proxy ingress enabled and sets the slug the
// operator builds the workload's own hostname from.
func withProxyIngressSlug(obj *unstructured.Unstructured, slug string) *unstructured.Unstructured {
	obj.Object["spec"].(map[string]any)["features"] = map[string]any{
		"appsProxyIngress": map[string]any{"targetPort": int64(8888), "slug": slug},
	}
	return obj
}

// An App that has never been promoted keeps spec.features.appsProxyIngress set
// while status.appsProxy stays null forever, so "no published hostname" is not
// a startup window there. It must not defend a hostname it would never publish.
func TestStateWatcher_ResolveHost_NeverPublishedAppDoesNotBlockUnrelatedDraft(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()

	// spec.features.appsProxyIngress set, status.appsProxy absent.
	appObj := withProxyIngressSlug(newAppObject("app-7327412", "7327412", k8sapp.AppActualStateRunning), "myapp")
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

	ref, claimed := watcher.ResolveHost(t.Context(), "draft-01a0aa03-4886-74b4-87c2-8518d0ebf7f1.hub.example.com")
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
