package k8sapp

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/metric"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// customHostsRecheckInterval is how often discovery is asked for the CustomAppDomain resource until it is served.
const customHostsRecheckInterval = 5 * time.Minute

const conditionRouted = "Routed"

// ResourceDiscovery lists the resources the cluster serves for a group version.
type ResourceDiscovery interface {
	ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error)
}

type customHostsDependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
}

// CustomHostRoute is the workload a custom host routes to. Host is the normalized index key.
type CustomHostRoute struct {
	Workload WorkloadRef
	Host     string
}

type claimRoute struct {
	name     string
	host     string
	appID    api.AppID
	routedAt time.Time
}

// CustomHosts indexes the hosts of CustomAppDomain claims that are Routed.
//
// The index never holds a host equal to or under apps-proxy's own public host, so a claim cannot shadow the .hub URL
// of another app. Without the CRD (or its RBAC) the index stays empty and the .hub routes keep working: nothing in
// readiness depends on it.
type CustomHosts struct {
	client    dynamic.Interface
	discovery ResourceDiscovery
	namespace string
	ownHost   string
	logger    log.Logger
	clock     clockwork.Clock
	metrics   *customHostsMetrics

	started  atomic.Bool
	informer atomic.Pointer[cache.SharedIndexInformer]

	lock   sync.RWMutex
	claims map[string]claimRoute // claim name → routed claim
	routes map[string]claimRoute // host → winning claim

	removedLock sync.Mutex
	removed     []func(WorkloadRef, string)
}

// NewCustomHosts starts watching the claims once discovery shows the CustomAppDomain resource,
// and until then checks again every five minutes. ownHost is apps-proxy's public host.
func NewCustomHosts(d customHostsDependencies, client dynamic.Interface, discovery ResourceDiscovery, namespace, ownHost string) *CustomHosts {
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel(nil)
	})

	c := &CustomHosts{
		client:    client,
		discovery: discovery,
		namespace: namespace,
		ownHost:   customHostKey(ownHost),
		logger:    d.Logger().WithComponent("k8sapp.customhosts"),
		clock:     d.Clock(),
		claims:    make(map[string]claimRoute),
		routes:    make(map[string]claimRoute),
	}
	c.metrics = newCustomHostsMetrics(d.Telemetry().Meter(), c.routeCount)

	go c.watchWhenServed(ctx)

	return c
}

// Resolve reports the workload that serves the host, accepting any spelling of it.
func (c *CustomHosts) Resolve(host string) (CustomHostRoute, bool) {
	key := customHostKey(host)

	c.lock.RLock()
	defer c.lock.RUnlock()

	route, ok := c.routes[key]
	if !ok {
		return CustomHostRoute{}, false
	}
	return CustomHostRoute{Workload: WorkloadRef{AppID: route.appID}, Host: key}, true
}

// OnHostRemoved registers a callback fired when a workload stops being served on a host:
// its claim is not Routed any more, is deleted, or lost the host to another claim.
func (c *CustomHosts) OnHostRemoved(fn func(ref WorkloadRef, host string)) {
	c.removedLock.Lock()
	defer c.removedLock.Unlock()
	c.removed = append(c.removed, fn)
}

// Started reports whether the claim informer runs.
func (c *CustomHosts) Started() bool {
	return c.started.Load()
}

// HasSynced reports whether the claim informer completed its initial list.
func (c *CustomHosts) HasSynced() bool {
	informer := c.informer.Load()
	return informer != nil && (*informer).HasSynced()
}

func (c *CustomHosts) routeCount() int64 {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return int64(len(c.routes))
}

func (c *CustomHosts) watchWhenServed(ctx context.Context) {
	for {
		if c.startIfServed(ctx) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-c.clock.After(customHostsRecheckInterval):
		}
	}
}

func (c *CustomHosts) startIfServed(ctx context.Context) bool {
	list, err := c.discovery.ServerResourcesForGroupVersion(Group + "/" + Version)
	if err != nil {
		c.logger.Warnf(ctx, "unable to discover the %s resource, custom hosts are not served: %s", CustomAppDomainResource, err)
		return false
	}
	if !slices.ContainsFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Name == CustomAppDomainResource }) {
		c.logger.Debugf(ctx, "the %s resource is not served, custom hosts are not served", CustomAppDomainResource)
		return false
	}

	informer := NewCRDInformer(c.client, CustomAppDomainGVR(), c.namespace)
	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.handleUpsert(ctx, obj) },
		UpdateFunc: func(_, obj any) { c.handleUpsert(ctx, obj) },
		DeleteFunc: func(obj any) { c.handleDelete(ctx, obj) },
	})
	if err != nil {
		c.logger.Errorf(ctx, "failed to add event handler to the %s informer: %s", CustomAppDomainResource, err)
		return false
	}

	c.informer.Store(&informer)
	c.started.Store(true)
	go informer.Run(ctx.Done())
	c.logger.Infof(ctx, "watching %s", CustomAppDomainResource)
	return true
}

type claimObject struct {
	Spec struct {
		Domain string `json:"domain"`
		AppID  string `json:"appId"`
	} `json:"spec"`
	Status struct {
		RoutedAt   *metav1.Time       `json:"routedAt,omitempty"`
		Conditions []metav1.Condition `json:"conditions,omitempty"`
	} `json:"status"`
}

func (c *CustomHosts) handleUpsert(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}

	var claim claimObject
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &claim); err != nil {
		c.logger.Errorf(ctx, "failed to convert %s %q: %s", CustomAppDomainResource, u.GetName(), err)
		c.apply(ctx, u.GetName(), nil)
		return
	}

	c.apply(ctx, u.GetName(), c.routedClaim(ctx, u, claim))
}

// routedClaim returns the claim's route, or nil when the claim is not served: it is not Routed, it is being deleted,
// it names no app or host, or its host belongs to apps-proxy itself.
func (c *CustomHosts) routedClaim(ctx context.Context, u *unstructured.Unstructured, claim claimObject) *claimRoute {
	if u.GetDeletionTimestamp() != nil || !isRouted(claim.Status.Conditions) {
		return nil
	}

	host := customHostKey(claim.Spec.Domain)
	if host == "" || claim.Spec.AppID == "" {
		c.logger.Warnf(ctx, "%s %q is Routed without a domain or an app id, skipping", CustomAppDomainResource, u.GetName())
		return nil
	}
	if c.isOwnHost(host) {
		c.logger.Errorf(ctx, "%s %q claims host %q under the proxy's own host %q, ignoring it", CustomAppDomainResource, u.GetName(), host, c.ownHost)
		return nil
	}

	route := claimRoute{name: u.GetName(), host: host, appID: api.AppID(claim.Spec.AppID)}
	if claim.Status.RoutedAt != nil {
		route.routedAt = claim.Status.RoutedAt.Time
	}
	return &route
}

func (c *CustomHosts) handleDelete(ctx context.Context, obj any) {
	u, ok := objectFromDeleteEvent(obj)
	if !ok {
		return
	}
	c.apply(ctx, u.GetName(), nil)
}

func isRouted(conditions []metav1.Condition) bool {
	for _, cond := range conditions {
		if cond.Type == conditionRouted {
			return cond.Status == metav1.ConditionTrue
		}
	}
	return false
}

func (c *CustomHosts) isOwnHost(host string) bool {
	return c.ownHost != "" && (host == c.ownHost || strings.HasSuffix(host, "."+c.ownHost))
}

type hostRemoval struct {
	ref  WorkloadRef
	host string
}

// apply replaces the claim's route (nil removes it) and recomputes the winner of every host the claim touched.
// The removal callbacks run after the index lock is released: they take handler locks that request goroutines
// hold while they read the index.
func (c *CustomHosts) apply(ctx context.Context, name string, next *claimRoute) {
	removals := c.update(ctx, name, next)
	for _, r := range removals {
		c.notifyRemoved(r)
	}
}

func (c *CustomHosts) update(ctx context.Context, name string, next *claimRoute) []hostRemoval {
	c.lock.Lock()
	defer c.lock.Unlock()

	var hosts []string
	if prev, ok := c.claims[name]; ok {
		hosts = append(hosts, prev.host)
	}
	if next == nil {
		delete(c.claims, name)
	} else {
		c.claims[name] = *next
		hosts = append(hosts, next.host)
	}

	removals := make([]hostRemoval, 0, len(hosts))
	for _, host := range slices.Compact(slices.Sorted(slices.Values(hosts))) {
		removals = append(removals, c.electLocked(ctx, host, next != nil && next.host == host)...)
	}
	return removals
}

// electLocked picks the claim that serves the host: the latest routedAt, the claim name breaking a tie.
// The caller must hold the lock.
func (c *CustomHosts) electLocked(ctx context.Context, host string, claimChanged bool) []hostRemoval {
	var winner claimRoute
	var count int
	for _, claim := range c.claims {
		if claim.host != host {
			continue
		}
		count++
		if count == 1 || claim.routedAt.After(winner.routedAt) || (claim.routedAt.Equal(winner.routedAt) && claim.name < winner.name) {
			winner = claim
		}
	}

	if count > 1 && claimChanged {
		c.metrics.conflict(ctx)
		c.logger.Errorf(ctx, "%d %s are Routed for host %q, claim %q (app %s) wins", count, CustomAppDomainResource, host, winner.name, winner.appID)
	}

	prev, hadRoute := c.routes[host]
	if count == 0 {
		delete(c.routes, host)
	} else {
		c.routes[host] = winner
	}

	if !hadRoute || (count > 0 && prev.appID == winner.appID) {
		return nil
	}
	return []hostRemoval{{ref: WorkloadRef{AppID: prev.appID}, host: host}}
}

func (c *CustomHosts) notifyRemoved(r hostRemoval) {
	c.removedLock.Lock()
	callbacks := slices.Clone(c.removed)
	c.removedLock.Unlock()

	for _, fn := range callbacks {
		fn(r.ref, r.host)
	}
}

// customHostKey is the index key of a host: lowercase, without port or trailing dot.
func customHostKey(host string) string {
	return strings.TrimSuffix(NormalizeHost(host), ".")
}

type customHostsMetrics struct {
	conflicts metric.Int64Counter
}

func newCustomHostsMetrics(meter telemetry.Meter, routes func() int64) *customHostsMetrics {
	meter.IntObservableGauge(
		"keboola.go.appsproxy.customhosts.routes",
		"Custom hosts the proxy routes: hosts of CustomAppDomain claims that are Routed.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(routes())
			return nil
		},
	)

	return &customHostsMetrics{
		conflicts: meter.IntCounter(
			"keboola.go.appsproxy.customhosts.conflicts",
			"Times more than one CustomAppDomain claim was Routed for the same host. The operator's lock should make it impossible.",
			"",
		),
	}
}

func (m *customHostsMetrics) conflict(ctx context.Context) {
	m.conflicts.Add(ctx, 1)
}
