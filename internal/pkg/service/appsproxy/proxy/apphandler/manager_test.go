package apphandler

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

// TestAppHandlerWrapper_NeedsRebuild verifies that handler recreation is keyed on
// config identity (the config ETag) and the upstream hash, not on a one-shot
// "modified" flag. A changed config ETag must trigger a rebuild even when the
// upstream hash is unchanged, so a replica that missed the single "modified"
// signal still converges to the new auth config.
func TestAppHandlerWrapper_NeedsRebuild(t *testing.T) {
	t.Parallel()

	w := &appHandlerWrapper{}
	key1 := upstreamKeyFor(k8sapp.AppInfo{PublicHost: "a.hub.example.com"}, true)
	key2 := upstreamKeyFor(k8sapp.AppInfo{PublicHost: "b.hub.example.com"}, true)

	// No handler yet → must build.
	assert.True(t, w.needsRebuild("etag-1", key1))

	// Simulate a built handler.
	w.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	w.configETag = "etag-1"
	w.upstream = key1

	// Nothing changed → reuse the cached handler.
	assert.False(t, w.needsRebuild("etag-1", key1))

	// Config ETag changed (auth config redeployed) → rebuild, even though the
	// upstream hash is unchanged. This is the core fix.
	assert.True(t, w.needsRebuild("etag-2", key1))

	// Upstream changed → rebuild.
	assert.True(t, w.needsRebuild("etag-1", key2))
}

// The handler cache is keyed by workload. A draft is short-lived, so without
// eviction the cache grows with every draft ever served and each stranded entry
// keeps a reverse proxy and an uncancelled context alive.
func TestManager_EvictWorkload(t *testing.T) {
	t.Parallel()

	m := newTestManager()

	ref := k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}

	cancelled := false
	wrapper := m.handlers.GetOrInit(handlerKey{Workload: ref})
	wrapper.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	wrapper.cancel = func(error) { cancelled = true }

	m.evictWorkload(ref)

	assert.True(t, cancelled, "the handler's context must be cancelled when its workload is gone")
	assert.Nil(t, m.handlers.GetOrInit(handlerKey{Workload: ref}).handler, "the entry must be gone, so the key re-initialises empty")
}

// A request takes the wrapper pointer before it takes the wrapper's lock. If the
// workload is evicted in that window, the request must not build a handler into
// the entry that eviction just detached from the map: that handler's cancel
// would never be reachable again.
func TestManager_EvictedWrapperIsNotRepopulated(t *testing.T) {
	t.Parallel()

	m := newTestManager()

	ref := k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}

	// The request has the pointer but has not locked it yet.
	stale := m.handlers.GetOrInit(handlerKey{Workload: ref})

	m.evictWorkload(ref)

	// The request now proceeds on the pointer it captured. It is answered
	// without the cache: the workload it named no longer exists.
	ctx := context.WithValue(t.Context(), middleware.RequestIDCtxKey, "test-request-id")
	assert.NotNil(t, m.handlerFor(ctx, appconfig.AppConfigResult{Workload: ref}, stale))
	assert.Nil(t, stale.handler, "nothing may be built into a detached entry")
}

// A Sandbox that republishes under a different hostname must rebuild its
// handler: the old one rewrites upstream redirects to the hostname it was built
// with, which would send the user to an address this workload no longer owns.
func TestUpstreamKey_ChangesWithThePublishedHostname(t *testing.T) {
	t.Parallel()

	before := upstreamKeyFor(k8sapp.AppInfo{PublicHost: "draft-abc.hub.example.com"}, true)
	after := upstreamKeyFor(k8sapp.AppInfo{PublicHost: "draft-xyz.hub.example.com"}, true)

	assert.NotEqual(t, before, after)
}

func TestUpstreamKey_ChangesWithTheUpstreamURL(t *testing.T) {
	t.Parallel()

	before := upstreamKeyFor(k8sapp.AppInfo{UpstreamTarget: &url.URL{Scheme: "http", Host: "app-1.svc:8888"}}, true)
	after := upstreamKeyFor(k8sapp.AppInfo{UpstreamTarget: &url.URL{Scheme: "http", Host: "app-2.svc:8888"}}, true)

	assert.NotEqual(t, before, after)
}

// A workload that becomes cached must rebuild even when it published nothing.
func TestUpstreamKey_ChangesWhenTheWorkloadBecomesCached(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, upstreamKeyFor(k8sapp.AppInfo{}, false), upstreamKeyFor(k8sapp.AppInfo{}, true))
}

type testHostRoutes struct {
	lock   sync.Mutex
	routes map[string]k8sapp.WorkloadRef
}

func (r *testHostRoutes) Resolve(host string) (k8sapp.CustomHostRoute, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	ref, ok := r.routes[host]
	return k8sapp.CustomHostRoute{Workload: ref, Host: host}, ok
}

func (r *testHostRoutes) drop(host string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.routes, host)
}

func customHostResult(ref k8sapp.WorkloadRef, host string) appconfig.AppConfigResult {
	return appconfig.AppConfigResult{Workload: ref, AppConfig: api.AppConfig{CustomHost: host}}
}

// A host that is not in the index creates no handler, so an evicted host cannot rebuild one.
func TestManager_UnroutedCustomHostCreatesNoHandler(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	m.customHosts = &testHostRoutes{routes: map[string]k8sapp.WorkloadRef{}}
	ref := k8sapp.WorkloadRef{AppID: "123"}

	ctx := context.WithValue(t.Context(), middleware.RequestIDCtxKey, "test-request-id")
	h := m.HandlerFor(ctx, customHostResult(ref, "gone.customer.org"))
	assert.NotNil(t, h)
	assert.Equal(t, 0, m.HandlerCount())
}

// The host leaves the index after the request resolved it and after the eviction ran, but before the request took
// the wrapper: the re-check under the wrapper lock must stop the build and remove the wrapper it created.
func TestManager_HostEvictedBetweenResolveAndBuild(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	routes := &testHostRoutes{routes: map[string]k8sapp.WorkloadRef{"data.customer.org": {AppID: "123"}}}
	m.customHosts = routes
	ref := k8sapp.WorkloadRef{AppID: "123"}
	result := customHostResult(ref, "data.customer.org")
	ctx := context.WithValue(t.Context(), middleware.RequestIDCtxKey, "test-request-id")

	// The request passed the first check and holds a fresh wrapper.
	assert.True(t, m.hostRouted(result))
	routes.drop("data.customer.org")
	m.EvictHost(ref, "data.customer.org") // finds nothing to delete
	wrapper := m.handlers.GetOrInit(handlerKeyFor(result))

	assert.NotNil(t, m.handlerFor(ctx, result, wrapper))
	assert.Nil(t, wrapper.handler, "nothing may be built for an unrouted host")
	assert.Equal(t, 0, m.HandlerCount(), "the wrapper must leave the cache")
}

// The host now belongs to another app: the workload is compared, not only the host.
func TestManager_HostMovedToAnotherApp(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	m.customHosts = &testHostRoutes{routes: map[string]k8sapp.WorkloadRef{"data.customer.org": {AppID: "999"}}}

	assert.False(t, m.hostRouted(customHostResult(k8sapp.WorkloadRef{AppID: "123"}, "data.customer.org")))
	assert.True(t, m.hostRouted(customHostResult(k8sapp.WorkloadRef{AppID: "999"}, "data.customer.org")))
	assert.True(t, m.hostRouted(appconfig.AppConfigResult{Workload: k8sapp.WorkloadRef{AppID: "123"}}), "the hub host needs no index")
}

// A handler that was already built is cancelled and cleared when the re-check fails: this request removes the wrapper
// from the cache before the eviction callback runs, so the callback finds nothing to cancel.
func TestManager_FailedRecheckCancelsABuiltHandler(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	routes := &testHostRoutes{routes: map[string]k8sapp.WorkloadRef{}}
	m.customHosts = routes
	ref := k8sapp.WorkloadRef{AppID: "123"}
	result := customHostResult(ref, "data.customer.org")

	cancelled := false
	wrapper := m.handlers.GetOrInit(handlerKeyFor(result))
	wrapper.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	wrapper.cancel = func(error) { cancelled = true }

	ctx := context.WithValue(t.Context(), middleware.RequestIDCtxKey, "test-request-id")
	assert.NotNil(t, m.handlerFor(ctx, result, wrapper))

	assert.True(t, cancelled, "the built handler must be cancelled")
	assert.Nil(t, wrapper.handler)
	assert.True(t, wrapper.evicted)
	assert.Equal(t, 0, m.HandlerCount())
}

// The claim informer calls EvictHost: a handler build that holds the wrapper lock must not block it,
// and the cancel runs once the build ends.
func TestManager_EvictHostDoesNotWaitForTheHandlerLock(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	ref := k8sapp.WorkloadRef{AppID: "123"}
	key := handlerKey{Workload: ref, Host: "data.customer.org"}

	cancelled := make(chan struct{})
	wrapper := m.handlers.GetOrInit(key)
	wrapper.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	wrapper.cancel = func(error) { close(cancelled) }

	wrapper.lock.Lock() // a handler build in progress
	returned := make(chan struct{})
	go func() {
		m.EvictHost(ref, "data.customer.org")
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("EvictHost blocked on the handler lock")
	}
	assert.Equal(t, 0, m.HandlerCount(), "the entry leaves the cache at once")
	select {
	case <-cancelled:
		t.Fatal("cancelled while the lock is held")
	default:
	}

	wrapper.lock.Unlock()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was never cancelled")
	}
}
