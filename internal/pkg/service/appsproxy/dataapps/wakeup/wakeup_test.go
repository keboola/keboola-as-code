package wakeup_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/wakeup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

const testNamespace = "keboola"

func newTestApp(appID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
			"kind":       "App",
			"metadata": map[string]any{
				"name":      "app-k8s-" + appID,
				"namespace": testNamespace,
			},
			"spec": map[string]any{
				"appId": appID,
			},
			"status": map[string]any{
				"currentState": string(k8sapp.AppActualStateStopped),
			},
		},
	}
}

func patchCount(actions []k8stesting.Action) int {
	count := 0
	for _, a := range actions {
		if a.GetVerb() == "patch" {
			count++
		}
	}
	return count
}

func TestManager_Wakeup(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedServiceScope(t, ctx, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("app")
	fakeClient := mock.TestFakeK8sClient()

	// Register app in fake K8s so SetDesiredRunning has a target.
	_, err := fakeClient.Resource(k8sapp.AppGVR).Namespace(testNamespace).Create(
		ctx, newTestApp(string(appID)), metav1.CreateOptions{},
	)
	require.NoError(t, err)

	manager := d.WakeupManager()
	watcher := d.AppStateWatcher()

	// Wait for watcher cache to sync.
	require.Eventually(t, func() bool {
		_, ok := watcher.GetState(appID)
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	// Clear list/watch actions from informer startup.
	fakeClient.ClearActions()

	// The first wakeup sends a K8s PATCH.
	err = manager.Wakeup(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 1, patchCount(fakeClient.Actions()))

	// Second call is skipped because the Interval was not exceeded.
	fakeClient.ClearActions()
	clk.Advance(time.Millisecond)
	err = manager.Wakeup(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 0, patchCount(fakeClient.Actions()))

	// After exceeding the Interval, a new PATCH is sent.
	fakeClient.ClearActions()
	clk.Advance(wakeup.Interval)
	err = manager.Wakeup(ctx, appID)
	require.NoError(t, err)
	assert.Equal(t, 1, patchCount(fakeClient.Actions()))
}

func TestManager_Wakeup_Race(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedServiceScope(t, ctx, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("app")
	fakeClient := mock.TestFakeK8sClient()

	// Register app in fake K8s.
	_, err := fakeClient.Resource(k8sapp.AppGVR).Namespace(testNamespace).Create(
		ctx, newTestApp(string(appID)), metav1.CreateOptions{},
	)
	require.NoError(t, err)

	manager := d.WakeupManager()
	watcher := d.AppStateWatcher()

	// Wait for watcher cache to sync.
	require.Eventually(t, func() bool {
		_, ok := watcher.GetState(appID)
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	// Clear list/watch actions from informer startup.
	fakeClient.ClearActions()

	raceCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	// Call Wakeup 10x in parallel.
	for range 10 {
		wg.Go(func() {
			require.NoError(t, manager.Wakeup(raceCtx, appID))
			counter.Add(1)
		})
	}

	wg.Wait()

	// All goroutines completed.
	assert.Equal(t, int64(10), counter.Load())
	// Only one K8s PATCH is sent due to rate-limiting.
	assert.Equal(t, 1, patchCount(fakeClient.Actions()))
}
