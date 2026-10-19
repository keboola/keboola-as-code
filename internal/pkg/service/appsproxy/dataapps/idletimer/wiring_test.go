package idletimer_test

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	wiringNamespace = "keboola"
	wiringApp       = "app-123"
	wiringMember    = "app-123-dpl-aaa"
)

// appObject is a Running App served by the member Sandbox.
func appObject() []runtime.Object {
	return []runtime.Object{
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
			"kind":       "App",
			"metadata":   map[string]any{"name": wiringApp, "namespace": wiringNamespace},
			"spec":       map[string]any{"appId": "123"},
			"status": map[string]any{
				"currentState":      string(k8sapp.AppActualStateRunning),
				"productionSandbox": wiringMember,
			},
		}},
	}
}

// memberObject is the member Sandbox, with the shortest threshold. Passed among
// the fake client's initial objects it never shows up in a list, so it is
// created once the service is up, as the proxy tests do.
func memberObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.SandboxVersion,
		"kind":       "Sandbox",
		"metadata":   map[string]any{"name": wiringMember, "namespace": wiringNamespace, "uid": "uid-member"},
		"spec":       map[string]any{"appId": "123", "autoSuspendAfterSeconds": int64(60)},
		"status": map[string]any{
			"currentState": string(k8sapp.AppActualStateRunning),
			"appsProxy":    map[string]any{"upstreamUrl": "http://app-123.keboola.svc.cluster.local"},
		},
	}}
}

type wiring struct {
	t     *testing.T
	scope proxyDependencies.ServiceScope
	fake  *k8sfake.FakeDynamicClient
	clock *clockwork.FakeClock
}

func newWiring(t *testing.T, suspendEnabled bool) *wiring {
	t.Helper()

	cfg := config.New()
	cfg.IdleSuspend.Enabled = suspendEnabled
	clock := clockwork.NewFakeClockAt(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))
	scope, mocked := proxyDependencies.NewMockedServiceScopeWithK8sObjects(t, t.Context(), cfg, appObject(), dependencies.WithClock(clock))
	fake := mocked.TestFakeK8sClient()

	_, err := fake.Resource(k8sapp.SandboxGVR()).Namespace(wiringNamespace).Create(t.Context(), memberObject(), metav1.CreateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(scope.AppStateWatcher().ScanForSleepCandidates().Candidates) == 1
	}, 5*time.Second, 10*time.Millisecond)

	return &wiring{t: t, scope: scope, fake: fake, clock: clock}
}

func (w *wiring) entries() map[string]any {
	obj, err := w.fake.Resource(k8sapp.IdleTimerGVR()).Namespace(wiringNamespace).Get(w.t.Context(), wiringMember, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	observers, _, _ := unstructured.NestedMap(obj.Object, "spec", "observers")
	return observers
}

// running advances the clock a second at a time, the cadence the sweeper ticks
// at, until done reports true.
func (w *wiring) running(done func() bool, msg string) {
	require.Eventually(w.t, func() bool {
		if done() {
			return true
		}
		w.clock.Advance(time.Second)
		return false
	}, 10*time.Second, 5*time.Millisecond, msg)
}

func (w *wiring) suspendPatched() bool {
	for _, a := range w.fake.Actions() {
		if p, ok := a.(k8stesting.PatchAction); ok && p.GetResource().Resource == k8sapp.AppGVR().Resource && p.GetName() == wiringApp {
			return true
		}
	}
	return false
}

// The final pass is what writes a replica's last marks and takes its entry out.
// It runs only if the service hooks the Recorder into the process shutdown.
func TestService_TakesItsEntryOutOfTheRecordOnShutdown(t *testing.T) {
	t.Parallel()

	w := newWiring(t, false)
	w.running(func() bool { return len(w.entries()) == 1 }, "the replica never wrote its entry")

	w.scope.Process().Shutdown(t.Context(), errors.New("test"))
	w.scope.Process().WaitForShutdown()

	assert.Empty(t, w.entries())
}

// The gate is the one switch between reporting a suspend and performing it, and
// it reaches the loop only through the service's constructor.
func TestService_PassesTheSuspendGateToTheLoop(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{true, false} {
		w := newWiring(t, enabled)
		start := w.clock.Now()
		w.running(func() bool { return w.suspendPatched() || w.clock.Since(start) > 2*time.Minute }, "the workload was never judged")
		assert.Equal(t, enabled, w.suspendPatched(), "suspend enabled=%t", enabled)
	}
}
