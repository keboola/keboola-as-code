package k8sapp_test

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

const testNamespace = "keboola"

// watcherDeps implements k8sapp.StateWatcher dependencies for tests.
type watcherDeps struct {
	logger log.Logger
	proc   *servicectx.Process
}

func newTestDeps(t *testing.T) *watcherDeps {
	t.Helper()
	logger := log.NewNopLogger()
	proc := servicectx.New(servicectx.WithLogger(logger), servicectx.WithoutSignals())
	t.Cleanup(func() {
		proc.Shutdown(context.Background(), nil)
		proc.WaitForShutdown()
	})
	return &watcherDeps{logger: logger, proc: proc}
}

func (d *watcherDeps) Logger() log.Logger           { return d.logger }
func (d *watcherDeps) Process() *servicectx.Process { return d.proc }

// newFakeClient creates a fake dynamic client with the App and Secret list kinds registered.
func newFakeClient() *k8sfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return k8sfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		k8sapp.AppGVR():    "AppList",
		k8sapp.SecretGVR(): "SecretList",
	})
}

// newAppObject creates an unstructured App CRD object.
func newAppObject(k8sName, appID string, state k8sapp.AppActualState) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
			"kind":       "App",
			"metadata": map[string]any{
				"name":      k8sName,
				"namespace": testNamespace,
			},
			"spec": map[string]any{
				"appId": appID,
			},
			"status": map[string]any{
				"currentState": string(state),
			},
		},
	}
}

// newAppObjectWithUpstreamURL creates an unstructured App CRD object with appsProxy.upstreamUrl set.
func newAppObjectWithUpstreamURL(k8sName, appID string, state k8sapp.AppActualState, upstreamURL string) *unstructured.Unstructured {
	obj := newAppObject(k8sName, appID, state)
	obj.Object["status"].(map[string]any)["appsProxy"] = map[string]any{"upstreamUrl": upstreamURL}
	return obj
}

func TestStateWatcher_GetState_UnknownWhenEmpty(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	info, ok := watcher.GetState(api.AppID("app-123"))
	assert.False(t, ok)
	assert.Empty(t, info.ActualState)
}

func TestStateWatcher_GetState_AfterCacheSync(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	// Add the App CRD object to the fake tracker before the watcher starts.
	appObj := newAppObject("my-app-k8s", "app-123", k8sapp.AppActualStateStopped)
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	assert.Eventually(t, func() bool {
		info, ok := watcher.GetState(api.AppID("app-123"))
		return ok && info.ActualState == k8sapp.AppActualStateStopped
	}, 5*time.Second, 50*time.Millisecond)
}

func TestStateWatcher_WakeupApp(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	appObj := newAppObject("my-app-k8s", "app-123", k8sapp.AppActualStateStopped)
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	// Wait for the informer to cache the object.
	require.Eventually(t, func() bool {
		_, ok := watcher.GetState(api.AppID("app-123"))
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	// Clear prior actions (list/watch from informer startup).
	fakeClient.ClearActions()

	err = watcher.WakeupApp(t.Context(), api.AppID("app-123"))
	require.NoError(t, err)

	// Verify that a merge-patch action targeting App CRDs was recorded.
	actions := fakeClient.Actions()
	require.Len(t, actions, 1)

	pa, ok := actions[0].(k8stesting.PatchAction)
	require.True(t, ok, "expected a PatchAction")
	assert.Equal(t, k8stypes.MergePatchType, pa.GetPatchType())
	assert.Contains(t, string(pa.GetPatch()), `"state":"Running"`)
}

func TestStateWatcher_WakeupApp_NoOpWhenUnknown(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	watcher := k8sapp.NewStateWatcher(newTestDeps(t), fakeClient, testNamespace)

	// App not in K8s cache — WakeupApp should be a no-op.
	err := watcher.WakeupApp(t.Context(), api.AppID("app-unknown"))
	require.NoError(t, err)

	for _, a := range fakeClient.Actions() {
		assert.NotEqual(t, "patch", a.GetVerb(), "unexpected PATCH for unknown app")
	}
}

func TestStateWatcher_GetState_UpstreamTarget(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	appObj := newAppObjectWithUpstreamURL("my-app-k8s", "app-123", k8sapp.AppActualStateRunning, "http://my-svc.keboola.svc.cluster.local:8888")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	var info k8sapp.AppInfo
	assert.Eventually(t, func() bool {
		var ok bool
		info, ok = watcher.GetState(api.AppID("app-123"))
		return ok && info.UpstreamTarget != nil
	}, 5*time.Second, 50*time.Millisecond)

	require.NotNil(t, info.UpstreamTarget)
	assert.Equal(t, "http", info.UpstreamTarget.Scheme)
	assert.Equal(t, "my-svc.keboola.svc.cluster.local:8888", info.UpstreamTarget.Host)
}

func TestStateWatcher_GetState_UpstreamTarget_AbsentWhenMissing(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	// App CRD without appsProxy.upstreamUrl.
	appObj := newAppObject("my-app-k8s", "app-123", k8sapp.AppActualStateRunning)
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(api.AppID("app-123"))
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	info, ok := watcher.GetState(api.AppID("app-123"))
	require.True(t, ok)
	assert.Nil(t, info.UpstreamTarget)
}

// newE2BAppObject creates an App CRD with spec.runtime.backend.type = "e2bSandbox"
// and status.e2bSandbox.accessTokenSecretName set.
func newE2BAppObject(k8sName, appID string, state k8sapp.AppActualState, secretName string) *unstructured.Unstructured {
	obj := newAppObject(k8sName, appID, state)
	obj.Object["spec"].(map[string]any)["runtime"] = map[string]any{
		"backend": map[string]any{
			"type": k8sapp.BackendTypeE2BSandbox,
		},
	}
	obj.Object["status"].(map[string]any)["e2bSandbox"] = map[string]any{
		"accessTokenSecretName": secretName,
	}
	return obj
}

// newSecretObject creates an unstructured K8s Secret with a base64-encoded "token" key.
func newSecretObject(name, namespace, tokenValue string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"data": map[string]any{
				"token": base64.StdEncoding.EncodeToString([]byte(tokenValue)),
			},
		},
	}
}

func TestStateWatcher_GetState_E2BAccessToken(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	// Create the secret first.
	secret := newSecretObject("my-e2b-secret", testNamespace, "my-e2b-token")
	_, err := fakeClient.Resource(k8sapp.SecretGVR()).Namespace(testNamespace).Create(
		t.Context(), secret, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	// Create E2B app referencing the secret.
	appObj := newE2BAppObject("my-e2b-app-k8s", "app-e2b", k8sapp.AppActualStateRunning, "my-e2b-secret")
	_, err = fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	var info k8sapp.AppInfo
	assert.Eventually(t, func() bool {
		var ok bool
		info, ok = watcher.GetState(api.AppID("app-e2b"))
		return ok && info.E2BAccessToken != ""
	}, 5*time.Second, 50*time.Millisecond)

	assert.Equal(t, "my-e2b-token", info.E2BAccessToken)
}

func TestStateWatcher_GetState_E2BAccessToken_MissingSecret(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	// Create E2B app referencing a non-existent secret.
	appObj := newE2BAppObject("my-e2b-app-k8s", "app-e2b", k8sapp.AppActualStateRunning, "missing-secret")
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(api.AppID("app-e2b"))
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	info, ok := watcher.GetState(api.AppID("app-e2b"))
	require.True(t, ok)
	assert.Empty(t, info.E2BAccessToken)
}

func TestStateWatcher_GetState_NonE2BApp_NoToken(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient()
	d := newTestDeps(t)

	// Create a regular (non-E2B) app.
	appObj := newAppObject("my-app-k8s", "app-regular", k8sapp.AppActualStateRunning)
	_, err := fakeClient.Resource(k8sapp.AppGVR()).Namespace(testNamespace).Create(
		t.Context(), appObj, metav1.CreateOptions{},
	)
	require.NoError(t, err)

	watcher := k8sapp.NewStateWatcher(d, fakeClient, testNamespace)

	assert.Eventually(t, func() bool {
		_, ok := watcher.GetState(api.AppID("app-regular"))
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	info, ok := watcher.GetState(api.AppID("app-regular"))
	require.True(t, ok)
	assert.Empty(t, info.E2BAccessToken)
}
