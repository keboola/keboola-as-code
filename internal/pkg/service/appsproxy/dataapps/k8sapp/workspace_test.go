package k8sapp_test

import (
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

const (
	draftName = "draft-abc"
	otherName = "app-123-dpl-bbb"
)

var (
	productionRef = k8sapp.WorkloadRef{AppID: memberAppID}
	draftRef      = k8sapp.WorkloadRef{AppID: memberAppID, SandboxName: draftName}
)

func withWorkspaceID(obj *unstructured.Unstructured, workspaceID string) *unstructured.Unstructured {
	obj.Object["status"].(map[string]any)["workspace"] = map[string]any{"id": workspaceID, "name": "Workspace " + workspaceID}
	return obj
}

func newNamedMemberSandbox(name, workspaceID string) *unstructured.Unstructured {
	obj := newSandboxObject(name, memberAppID, k8sapp.AppActualStateRunning, "", prodUpstreamURL)
	if workspaceID != "" {
		withWorkspaceID(obj, workspaceID)
	}
	return obj
}

func newDraftSandbox(workspaceID string) *unstructured.Unstructured {
	obj := newSandboxObject(draftName, memberAppID, k8sapp.AppActualStateRunning, "https://draft-abc.hub.example.com", "http://a:8888")
	if workspaceID != "" {
		withWorkspaceID(obj, workspaceID)
	}
	return obj
}

func workspaceWatcher(t *testing.T, logger log.Logger, objects ...*unstructured.Unstructured) (*k8sapp.StateWatcher, *k8sfake.FakeDynamicClient) {
	t.Helper()
	fakeClient := newFakeClient()
	for _, obj := range objects {
		gvr := k8sapp.AppGVR()
		if obj.GetKind() == "Sandbox" {
			gvr = k8sapp.SandboxGVR()
		}
		_, err := fakeClient.Resource(gvr).Namespace(testNamespace).Create(t.Context(), obj, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	watcher := k8sapp.NewStateWatcher(newTestDepsWithLogger(t, logger), fakeClient, testNamespace)
	require.True(t, watcher.WaitForCacheSync(t.Context()))
	return watcher, fakeClient
}

func update(t *testing.T, client *k8sfake.FakeDynamicClient, obj *unstructured.Unstructured) {
	t.Helper()
	gvr := k8sapp.AppGVR()
	if obj.GetKind() == "Sandbox" {
		gvr = k8sapp.SandboxGVR()
	}
	_, err := client.Resource(gvr).Namespace(testNamespace).Update(t.Context(), obj, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func assertWorkspaceEventually(t *testing.T, watcher *k8sapp.StateWatcher, ref k8sapp.WorkloadRef, expected string) {
	t.Helper()
	assert.Eventually(t, func() bool {
		info, ok := watcher.GetState(t.Context(), ref)
		return ok && info.WorkspaceID == expected
	}, 5*time.Second, 50*time.Millisecond)
}

func TestGetState_WorkspaceID_ProductionComesFromTheProductionSandbox(t *testing.T) {
	t.Parallel()

	watcher, _ := workspaceWatcher(t, log.NewNopLogger(),
		newNamedMemberSandbox(memberName, "91011"),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, memberName),
	)

	assertWorkspaceEventually(t, watcher, productionRef, "91011")
}

func TestGetState_WorkspaceID_DraftComesFromItsOwnSandbox(t *testing.T) {
	t.Parallel()

	watcher, _ := workspaceWatcher(t, log.NewNopLogger(),
		newNamedMemberSandbox(memberName, "91011"),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, memberName),
		newDraftSandbox("555"),
	)

	assertWorkspaceEventually(t, watcher, draftRef, "555")
}

func TestGetState_WorkspaceID_EmptyWithoutLogWhenUnresolved(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		objects []*unstructured.Unstructured
		ref     k8sapp.WorkloadRef
	}{
		{
			name:    "production sandbox not cached",
			objects: []*unstructured.Unstructured{withWorkspaceID(newAppWithProductionSandbox(k8sapp.AppActualStateRunning, "app-123-dpl-gone"), "999")},
			ref:     productionRef,
		},
		{
			name:    "no production sandbox",
			objects: []*unstructured.Unstructured{withWorkspaceID(newAppWithProductionSandbox(k8sapp.AppActualStateRunning, ""), "999")},
			ref:     productionRef,
		},
		{
			name: "production sandbox without workspace",
			objects: []*unstructured.Unstructured{
				newNamedMemberSandbox(memberName, ""),
				withWorkspaceID(newAppWithProductionSandbox(k8sapp.AppActualStateRunning, memberName), "999"),
			},
			ref: productionRef,
		},
		{
			name:    "draft without workspace",
			objects: []*unstructured.Unstructured{newDraftSandbox("")},
			ref:     draftRef,
		},
		{
			name:    "draft workspace without id",
			objects: []*unstructured.Unstructured{withWorkspaceID(newDraftSandbox(""), "")},
			ref:     draftRef,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := log.NewDebugLogger()
			watcher, _ := workspaceWatcher(t, logger, tc.objects...)

			var info k8sapp.AppInfo
			require.Eventually(t, func() bool {
				var ok bool
				info, ok = watcher.GetState(t.Context(), tc.ref)
				return ok
			}, 5*time.Second, 50*time.Millisecond)

			assert.Empty(t, info.WorkspaceID)
			assert.Empty(t, logger.WarnAndErrorMessages())
		})
	}
}

func TestGetState_WorkspaceID_FollowsPromotion(t *testing.T) {
	t.Parallel()

	watcher, client := workspaceWatcher(t, log.NewNopLogger(),
		newNamedMemberSandbox(memberName, "91011"),
		newNamedMemberSandbox(otherName, "121314"),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, memberName),
	)
	assertWorkspaceEventually(t, watcher, productionRef, "91011")

	update(t, client, newAppWithProductionSandbox(k8sapp.AppActualStateRunning, otherName))

	assertWorkspaceEventually(t, watcher, productionRef, "121314")
}

func TestGetState_WorkspaceID_FollowsTheSandbox(t *testing.T) {
	t.Parallel()

	watcher, client := workspaceWatcher(t, log.NewNopLogger(),
		newNamedMemberSandbox(memberName, ""),
		newAppWithProductionSandbox(k8sapp.AppActualStateRunning, memberName),
		newDraftSandbox(""),
	)
	assertWorkspaceEventually(t, watcher, productionRef, "")
	assertWorkspaceEventually(t, watcher, draftRef, "")

	update(t, client, newNamedMemberSandbox(memberName, "91011"))
	update(t, client, newDraftSandbox("555"))
	assertWorkspaceEventually(t, watcher, productionRef, "91011")
	assertWorkspaceEventually(t, watcher, draftRef, "555")

	update(t, client, newNamedMemberSandbox(memberName, "121314"))
	assertWorkspaceEventually(t, watcher, productionRef, "121314")
}
