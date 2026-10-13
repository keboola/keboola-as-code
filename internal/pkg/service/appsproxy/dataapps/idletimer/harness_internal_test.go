package idletimer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const threshold = 900 * time.Second

var appRef = k8sapp.WorkloadRef{AppID: "123"} //nolint:gochecknoglobals // test fixture

// fakeSource stands in for the K8s state watcher.
type fakeSource struct {
	synced     bool
	unresolved []api.AppID
	candidates []k8sapp.SleepCandidate
	slept      []k8sapp.WorkloadRef
	sleptOK    bool
	sleepErr   error
}

func (f *fakeSource) HasSynced() bool { return f.synced }

func (f *fakeSource) ScanForSleepCandidates() k8sapp.SleepScan {
	return k8sapp.SleepScan{Candidates: f.candidates, Unresolved: f.unresolved}
}

func (f *fakeSource) Sleep(_ context.Context, ref k8sapp.WorkloadRef, _ string) (bool, error) {
	if f.sleepErr != nil {
		return false, f.sleepErr
	}
	f.slept = append(f.slept, ref)
	return f.sleptOK, nil
}

type harness struct {
	t       *testing.T
	manager *Manager
	source  *fakeSource
	clock   *clockwork.FakeClock
	fake    *k8sfake.FakeDynamicClient
	tel     telemetry.ForTest
	store   cache.Store
	synced  *atomic.Bool
}

// cacheRecord puts a record in the watch cache without going near the
// apiserver, so a test can say what the cache holds and what the apiserver
// holds separately.
func (h *harness) cacheRecord(name string, at time.Time) {
	require.NoError(h.t, h.store.Add(&unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": k8sapp.Group + "/" + version,
			"kind":       kind,
			"metadata": map[string]any{
				"name":            name,
				"namespace":       testNamespace,
				"resourceVersion": "1",
			},
			"spec": map[string]any{"lastRequestAt": formatTime(at)},
		},
	}))
}

// counterValue reads an emitted counter back, so the assertions are about what
// a dashboard would show rather than about a field kept for the tests.
// suspendCount reads one outcome of the suspends counter. Taking the first
// data point would read whichever outcome happened to be recorded first.
func (h *harness) suspendCount(t *testing.T, outcome string) int64 {
	t.Helper()
	for _, m := range h.tel.Metrics(t) {
		if m.Name != "keboola.go.appsproxy.idletimer.suspends" {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			return 0
		}
		for _, dp := range sum.DataPoints {
			if v, found := dp.Attributes.Value(attribute.Key("outcome")); found && v.AsString() == outcome {
				return dp.Value
			}
		}
	}
	return 0
}

// flush waits for the detached writes. Shutdown also waits, but it stops
// admitting new ones, so a test that writes again afterwards cannot use it.
func (h *harness) flush() {
	h.manager.wg.Wait()
}

func (h *harness) counterValue(t *testing.T, name string) int64 {
	t.Helper()
	for _, m := range h.tel.Metrics(t) {
		if m.Name != name {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok || len(sum.DataPoints) == 0 {
			return 0
		}
		return sum.DataPoints[0].Value
	}
	return 0
}

// newHarness builds a manager with the suspend action on, which is what the
// loop's own behaviour has to be correct for once the gate is flipped.
func newHarness(t *testing.T, candidates ...k8sapp.SleepCandidate) *harness {
	t.Helper()
	return buildHarness(t, log.NewNopLogger(), true, candidates)
}

func newGatedHarness(t *testing.T, candidates ...k8sapp.SleepCandidate) *harness {
	t.Helper()
	return buildHarness(t, log.NewNopLogger(), false, candidates)
}

func newGatedHarnessWithLogger(t *testing.T, logger log.Logger, candidates ...k8sapp.SleepCandidate) *harness {
	t.Helper()
	return buildHarness(t, logger, false, candidates)
}

func buildHarness(t *testing.T, logger log.Logger, suspendEnabled bool, candidates []k8sapp.SleepCandidate) *harness {
	t.Helper()

	c := newTestClient()
	source := &fakeSource{synced: true, candidates: candidates, sleptOK: true}
	clock := clockwork.NewFakeClockAt(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))
	tel := telemetry.NewForTest(t)

	m := newManager(t.Context(), clock, logger, c, source, newMetrics(tel.Meter()), suspendEnabled)

	synced := &atomic.Bool{}
	synced.Store(true)
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	m.records = &recordCache{namespace: testNamespace, store: store, hasSynced: synced.Load}

	return &harness{
		t:       t,
		manager: m,
		source:  source,
		clock:   clock,
		fake:    c.dyn.(*k8sfake.FakeDynamicClient),
		tel:     tel,
		store:   store,
		synced:  synced,
	}
}

func candidate(ref k8sapp.WorkloadRef, sandbox string) k8sapp.SleepCandidate {
	t := threshold
	return k8sapp.SleepCandidate{Ref: ref, SandboxName: sandbox, Threshold: &t}
}

// optedOut is a workload whose Sandbox carries no threshold at all.
func optedOut(ref k8sapp.WorkloadRef, sandbox string) k8sapp.SleepCandidate {
	return k8sapp.SleepCandidate{Ref: ref, SandboxName: sandbox}
}
