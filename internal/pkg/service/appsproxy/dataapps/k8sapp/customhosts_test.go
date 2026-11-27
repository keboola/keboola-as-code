package k8sapp_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const testOwnHost = "hub.example.com"

type customHostsDeps struct {
	*watcherDeps
	clock *clockwork.FakeClock
	tel   telemetry.ForTest
}

func (d *customHostsDeps) Clock() clockwork.Clock         { return d.clock }
func (d *customHostsDeps) Telemetry() telemetry.Telemetry { return d.tel }

func newCustomHostsDeps(t *testing.T) *customHostsDeps {
	t.Helper()
	proc := servicectx.New(servicectx.WithLogger(log.NewNopLogger()), servicectx.WithoutSignals())
	t.Cleanup(func() {
		proc.Shutdown(context.Background(), nil)
		proc.WaitForShutdown()
	})
	return &customHostsDeps{
		watcherDeps: &watcherDeps{logger: log.NewNopLogger(), proc: proc},
		clock:       clockwork.NewFakeClock(),
		tel:         telemetry.NewForTest(t),
	}
}

// stubDiscovery lists the resources of apps.keboola.com/v2 that the cluster serves.
type stubDiscovery struct {
	lock      sync.Mutex
	resources []string
	err       error
	calls     int
}

func (d *stubDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.calls++
	if d.err != nil {
		return nil, d.err
	}
	list := &metav1.APIResourceList{GroupVersion: groupVersion}
	for _, name := range d.resources {
		list.APIResources = append(list.APIResources, metav1.APIResource{Name: name})
	}
	return list, nil
}

func (d *stubDiscovery) serve(resources ...string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.resources = resources
}

func (d *stubDiscovery) callCount() int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.calls
}

func newDiscoveryWithClaims() *stubDiscovery {
	return &stubDiscovery{resources: []string{"apps", "customappdomains"}}
}

// newClaimFakeClient has no CustomAppDomain list kind registered: the fake client panics on a list of an unregistered
// resource, so a watcher that starts the informer without the resource being served fails the test.
func newClaimFakeClient(withClaimKind bool, objects ...runtime.Object) *k8sfake.FakeDynamicClient {
	kinds := map[schema.GroupVersionResource]string{
		k8sapp.AppGVR():     "AppList",
		k8sapp.SandboxGVR(): "SandboxList",
	}
	if withClaimKind {
		kinds[k8sapp.CustomAppDomainGVR()] = "CustomAppDomainList"
	}
	return k8sfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds, objects...)
}

type claimOpts struct {
	routed     bool
	routedAt   time.Time
	appID      string
	deleting   bool
	noRoutedAt bool
}

func newClaim(name, domain string, opts claimOpts) *unstructured.Unstructured {
	status := "False"
	if opts.routed {
		status = "True"
	}
	appID := opts.appID
	if appID == "" {
		appID = "123"
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
		"kind":       "CustomAppDomain",
		"metadata":   map[string]any{"name": name, "namespace": testNamespace},
		"spec":       map[string]any{"domain": domain, "appId": appID, "appRef": "app-" + appID},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Routed", "status": status, "reason": "EdgeAcquired"},
			},
		},
	}}
	if !opts.noRoutedAt {
		routedAt := opts.routedAt
		if routedAt.IsZero() {
			routedAt = time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
		}
		obj.Object["status"].(map[string]any)["routedAt"] = routedAt.Format(time.RFC3339)
	}
	if opts.deleting {
		obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
	}
	return obj
}

type removal struct {
	ref  k8sapp.WorkloadRef
	host string
}

type removals struct {
	lock sync.Mutex
	list []removal
}

func (r *removals) add(ref k8sapp.WorkloadRef, host string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.list = append(r.list, removal{ref: ref, host: host})
}

func (r *removals) all() []removal {
	r.lock.Lock()
	defer r.lock.Unlock()
	return append([]removal(nil), r.list...)
}

func startCustomHosts(t *testing.T, d *customHostsDeps, client *k8sfake.FakeDynamicClient, discovery k8sapp.ResourceDiscovery, ownHost string) (*k8sapp.CustomHosts, *removals) {
	t.Helper()
	hosts := k8sapp.NewCustomHosts(d, client, discovery, testNamespace, ownHost)
	gone := &removals{}
	hosts.OnHostRemoved(gone.add)
	return hosts, gone
}

func requireSynced(t *testing.T, hosts *k8sapp.CustomHosts) {
	t.Helper()
	require.Eventually(t, hosts.HasSynced, 5*time.Second, 10*time.Millisecond)
}

func routeTo(hosts *k8sapp.CustomHosts, host string) (string, bool) {
	route, ok := hosts.Resolve(host)
	return route.Workload.AppID.String(), ok
}

func updateClaim(t *testing.T, client *k8sfake.FakeDynamicClient, obj *unstructured.Unstructured) {
	t.Helper()
	_, err := client.Resource(k8sapp.CustomAppDomainGVR()).Namespace(testNamespace).Update(t.Context(), obj, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func TestCustomHosts_RoutedClaimResolvesToTheApp(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true, newClaim("123-aaaa", "App.Customer.Example", claimOpts{routed: true}))
	hosts, _ := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "app.customer.example"); return ok }, 5*time.Second, 10*time.Millisecond)

	for _, spelling := range []string{"app.customer.example", "APP.Customer.Example", "app.customer.example:8443"} {
		route, ok := hosts.Resolve(spelling)
		require.True(t, ok, spelling)
		assert.Equal(t, k8sapp.WorkloadRef{AppID: "123"}, route.Workload, spelling)
		assert.Equal(t, "app.customer.example", route.Host, spelling)
	}

	_, ok := hosts.Resolve("other.customer.example")
	assert.False(t, ok)
}

func TestCustomHosts_ClaimWithoutRoutedIsNotIndexed(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true,
		newClaim("123-aaaa", "unrouted.example", claimOpts{routed: false}),
		newClaim("123-bbbb", "deleting.example", claimOpts{routed: true, deleting: true}),
		newClaim("123-cccc", "routed.example", claimOpts{routed: true}),
	)
	hosts, _ := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "routed.example"); return ok }, 5*time.Second, 10*time.Millisecond)

	_, ok := hosts.Resolve("unrouted.example")
	assert.False(t, ok)
	_, ok = hosts.Resolve("deleting.example")
	assert.False(t, ok)
}

func TestCustomHosts_ClaimWithoutAppIDIsNotIndexed(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	noApp := newClaim("123-aaaa", "noapp.example", claimOpts{routed: true})
	unstructured.RemoveNestedField(noApp.Object, "spec", "appId")
	client := newClaimFakeClient(true, noApp, newClaim("123-bbbb", "ok.example", claimOpts{routed: true}))
	hosts, _ := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "ok.example"); return ok }, 5*time.Second, 10*time.Millisecond)
	_, ok := hosts.Resolve("noapp.example")
	assert.False(t, ok)
}

func TestCustomHosts_OwnHostGuard(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true,
		newClaim("123-aaaa", "hub.example.com", claimOpts{routed: true}),
		newClaim("123-bbbb", "victim-456.hub.example.com", claimOpts{routed: true, appID: "999"}),
		newClaim("123-cccc", "VICTIM-456.HUB.EXAMPLE.COM", claimOpts{routed: true, appID: "999"}),
		newClaim("123-dddd", "deep.victim-456.hub.example.com", claimOpts{routed: true, appID: "999"}),
		newClaim("123-eeee", "nothub.example.com", claimOpts{routed: true, appID: "7"}),
		newClaim("123-ffff", "myhub.example.com", claimOpts{routed: true, appID: "8"}),
	)
	// The configured public host carries a port, the guard compares host names only.
	hosts, _ := startCustomHosts(t, d, client, newDiscoveryWithClaims(), "Hub.Example.com:8443")

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "nothub.example.com"); return ok }, 5*time.Second, 10*time.Millisecond)
	requireSynced(t, hosts)

	for _, host := range []string{"hub.example.com", "victim-456.hub.example.com", "deep.victim-456.hub.example.com"} {
		_, ok := hosts.Resolve(host)
		assert.False(t, ok, host)
	}
	app, ok := routeTo(hosts, "myhub.example.com")
	assert.True(t, ok)
	assert.Equal(t, "8", app)
}

func TestCustomHosts_ClaimLeavingRoutedIsEvicted(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true, newClaim("123-aaaa", "app.customer.example", claimOpts{routed: true}))
	hosts, gone := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)
	requireSynced(t, hosts)
	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "app.customer.example"); return ok }, 5*time.Second, 10*time.Millisecond)

	updateClaim(t, client, newClaim("123-aaaa", "app.customer.example", claimOpts{routed: false}))

	require.Eventually(t, func() bool { return len(gone.all()) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, []removal{{ref: k8sapp.WorkloadRef{AppID: "123"}, host: "app.customer.example"}}, gone.all())
	_, ok := hosts.Resolve("app.customer.example")
	assert.False(t, ok)
}

func TestCustomHosts_DeletedClaimIsEvicted(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true, newClaim("123-aaaa", "app.customer.example", claimOpts{routed: true}))
	hosts, gone := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)
	requireSynced(t, hosts)
	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "app.customer.example"); return ok }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, client.Resource(k8sapp.CustomAppDomainGVR()).Namespace(testNamespace).Delete(t.Context(), "123-aaaa", metav1.DeleteOptions{}))

	require.Eventually(t, func() bool { return len(gone.all()) == 1 }, 5*time.Second, 10*time.Millisecond)
	_, ok := hosts.Resolve("app.customer.example")
	assert.False(t, ok)
}

func TestCustomHosts_ConflictLatestRoutedAtWins(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true,
		newClaim("111-aaaa", "shared.example", claimOpts{routed: true, appID: "111", routedAt: older}),
		newClaim("222-aaaa", "shared.example", claimOpts{routed: true, appID: "222", routedAt: newer}),
	)
	hosts, gone := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)
	requireSynced(t, hosts)

	require.Eventually(t, func() bool { app, _ := routeTo(hosts, "shared.example"); return app == "222" }, 5*time.Second, 10*time.Millisecond)

	// The winner leaves: the other routed claim takes the host over, and the winner's handler is evicted.
	require.NoError(t, client.Resource(k8sapp.CustomAppDomainGVR()).Namespace(testNamespace).Delete(t.Context(), "222-aaaa", metav1.DeleteOptions{}))
	require.Eventually(t, func() bool { app, _ := routeTo(hosts, "shared.example"); return app == "111" }, 5*time.Second, 10*time.Millisecond)

	removed := gone.all()
	evictedApps := make([]string, 0, len(removed))
	for _, r := range removed {
		assert.Equal(t, "shared.example", r.host)
		evictedApps = append(evictedApps, r.ref.AppID.String())
	}
	assert.Contains(t, evictedApps, "222")

	// The loser leaves while it is not the winner: the winner stays.
	d2 := newCustomHostsDeps(t)
	client2 := newClaimFakeClient(true,
		newClaim("111-aaaa", "shared.example", claimOpts{routed: true, appID: "111", routedAt: older}),
		newClaim("222-aaaa", "shared.example", claimOpts{routed: true, appID: "222", routedAt: newer}),
	)
	hosts2, gone2 := startCustomHosts(t, d2, client2, newDiscoveryWithClaims(), testOwnHost)
	requireSynced(t, hosts2)
	require.Eventually(t, func() bool { app, _ := routeTo(hosts2, "shared.example"); return app == "222" }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, client2.Resource(k8sapp.CustomAppDomainGVR()).Namespace(testNamespace).Delete(t.Context(), "111-aaaa", metav1.DeleteOptions{}))
	require.Eventually(t, func() bool {
		_, err := client2.Resource(k8sapp.CustomAppDomainGVR()).Namespace(testNamespace).Get(t.Context(), "111-aaaa", metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 5*time.Second, 10*time.Millisecond)
	assert.Never(t, func() bool { app, _ := routeTo(hosts2, "shared.example"); return app != "222" }, 200*time.Millisecond, 10*time.Millisecond)
	for _, r := range gone2.all() {
		assert.NotEqual(t, "222", r.ref.AppID.String(), "the winner must not be evicted when the loser leaves")
	}
}

func TestCustomHosts_ConflictWithoutRoutedAtIsDeterministic(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true,
		newClaim("111-aaaa", "shared.example", claimOpts{routed: true, appID: "111", noRoutedAt: true}),
		newClaim("222-aaaa", "shared.example", claimOpts{routed: true, appID: "222", noRoutedAt: true}),
	)
	hosts, _ := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)
	requireSynced(t, hosts)

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "shared.example"); return ok }, 5*time.Second, 10*time.Millisecond)
	first, _ := routeTo(hosts, "shared.example")
	for range 5 {
		app, _ := routeTo(hosts, "shared.example")
		assert.Equal(t, first, app)
	}
}

func TestCustomHosts_ConflictMetric(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true,
		newClaim("111-aaaa", "shared.example", claimOpts{routed: true, appID: "111"}),
		newClaim("222-aaaa", "shared.example", claimOpts{routed: true, appID: "222"}),
		newClaim("333-aaaa", "alone.example", claimOpts{routed: true, appID: "333"}),
	)
	hosts, _ := startCustomHosts(t, d, client, newDiscoveryWithClaims(), testOwnHost)
	requireSynced(t, hosts)

	require.Eventually(t, func() bool {
		return metricSum(t, d.tel, "keboola.go.appsproxy.customhosts.conflicts") > 0
	}, 5*time.Second, 20*time.Millisecond)
	assert.Equal(t, int64(2), metricGauge(t, d.tel, "keboola.go.appsproxy.customhosts.routes"))
}

func metricSum(t *testing.T, tel telemetry.ForTest, name string) int64 {
	t.Helper()
	var total int64
	for _, m := range tel.Metrics(t) {
		if m.Name != name {
			continue
		}
		switch data := m.Data.(type) {
		case metricdata.Sum[int64]:
			for _, p := range data.DataPoints {
				total += p.Value
			}
		case metricdata.Gauge[int64]:
			for _, p := range data.DataPoints {
				total += p.Value
			}
		}
	}
	return total
}

func metricGauge(t *testing.T, tel telemetry.ForTest, name string) int64 {
	t.Helper()
	return metricSum(t, tel, name)
}

func TestCustomHosts_MissingCRDDoesNotStartTheInformer(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	// No CustomAppDomain list kind: listing it would panic, so this test fails if the informer starts.
	client := newClaimFakeClient(false)
	discovery := &stubDiscovery{resources: []string{"apps", "sandboxes"}}
	hosts, _ := startCustomHosts(t, d, client, discovery, testOwnHost)

	require.Eventually(t, func() bool { return discovery.callCount() >= 1 }, 5*time.Second, 10*time.Millisecond)

	_, ok := hosts.Resolve("app.customer.example")
	assert.False(t, ok)
	assert.False(t, hosts.Started())
	assert.False(t, hosts.HasSynced())
}

func TestCustomHosts_DiscoveryErrorKeepsRetrying(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true, newClaim("123-aaaa", "app.customer.example", claimOpts{routed: true}))
	discovery := newDiscoveryWithClaims()
	discovery.err = assert.AnError
	hosts, _ := startCustomHosts(t, d, client, discovery, testOwnHost)

	require.Eventually(t, func() bool { return discovery.callCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.False(t, hosts.Started())

	discovery.lock.Lock()
	discovery.err = nil
	discovery.lock.Unlock()
	require.NoError(t, d.clock.BlockUntilContext(t.Context(), 1))
	d.clock.Advance(5 * time.Minute)

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "app.customer.example"); return ok }, 5*time.Second, 10*time.Millisecond)
}

func TestCustomHosts_StartsWhenTheCRDAppears(t *testing.T) {
	t.Parallel()

	d := newCustomHostsDeps(t)
	client := newClaimFakeClient(true, newClaim("123-aaaa", "app.customer.example", claimOpts{routed: true}))
	discovery := &stubDiscovery{resources: []string{"apps"}}
	hosts, _ := startCustomHosts(t, d, client, discovery, testOwnHost)

	require.Eventually(t, func() bool { return discovery.callCount() == 1 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, d.clock.BlockUntilContext(t.Context(), 1))

	// Four minutes: not yet re-checked.
	d.clock.Advance(4 * time.Minute)
	assert.Never(t, func() bool { return discovery.callCount() > 1 }, 100*time.Millisecond, 10*time.Millisecond)

	discovery.serve("apps", "customappdomains")
	d.clock.Advance(time.Minute)

	require.Eventually(t, func() bool { _, ok := routeTo(hosts, "app.customer.example"); return ok }, 5*time.Second, 10*time.Millisecond)
	assert.True(t, hosts.Started())

	// The informer runs once: discovery is not polled any more.
	calls := discovery.callCount()
	d.clock.Advance(time.Hour)
	assert.Never(t, func() bool { return discovery.callCount() != calls }, 100*time.Millisecond, 10*time.Millisecond)
}
