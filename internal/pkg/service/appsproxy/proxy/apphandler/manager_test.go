package apphandler

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
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

	// No handler yet → must build.
	assert.True(t, w.needsRebuild("etag-1", "hash-1"))

	// Simulate a built handler.
	w.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	w.configETag = "etag-1"
	w.handlerHash = "hash-1"

	// Nothing changed → reuse the cached handler.
	assert.False(t, w.needsRebuild("etag-1", "hash-1"))

	// Config ETag changed (auth config redeployed) → rebuild, even though the
	// upstream hash is unchanged. This is the core fix.
	assert.True(t, w.needsRebuild("etag-2", "hash-1"))

	// Upstream/E2B hash changed → rebuild.
	assert.True(t, w.needsRebuild("etag-1", "hash-2"))
}

// The handler cache is keyed by workload. A draft is short-lived, so without
// eviction the cache grows with every draft ever served and each stranded entry
// keeps a reverse proxy and an uncancelled context alive.
func TestManager_EvictWorkload(t *testing.T) {
	t.Parallel()

	m := &Manager{
		handlers: syncmap.New[k8sapp.WorkloadRef, appHandlerWrapper](func(k8sapp.WorkloadRef) *appHandlerWrapper {
			return &appHandlerWrapper{lock: &sync.Mutex{}}
		}),
	}

	ref := k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}

	cancelled := false
	wrapper := m.handlers.GetOrInit(ref)
	wrapper.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	wrapper.cancel = func(error) { cancelled = true }

	m.evictWorkload(ref)

	assert.True(t, cancelled, "the handler's context must be cancelled when its workload is gone")
	assert.Nil(t, m.handlers.GetOrInit(ref).handler, "the entry must be gone, so the key re-initialises empty")
}

// A request takes the wrapper pointer before it takes the wrapper's lock. If the
// workload is evicted in that window, the request must not build a handler into
// the entry that eviction just detached from the map: that handler's cancel
// would never be reachable again.
func TestManager_EvictedWrapperIsNotRepopulated(t *testing.T) {
	t.Parallel()

	m := &Manager{
		handlers: syncmap.New[k8sapp.WorkloadRef, appHandlerWrapper](func(k8sapp.WorkloadRef) *appHandlerWrapper {
			return &appHandlerWrapper{lock: &sync.Mutex{}}
		}),
	}

	ref := k8sapp.WorkloadRef{AppID: "123", SandboxName: "draft-abc"}

	// The request has the pointer but has not locked it yet.
	stale := m.handlers.GetOrInit(ref)

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
func TestHandlerHash_ChangesWithThePublishedHostname(t *testing.T) {
	t.Parallel()

	before := handlerHash(k8sapp.AppInfo{PublicHost: "draft-abc.hub.example.com"}, true)
	after := handlerHash(k8sapp.AppInfo{PublicHost: "draft-xyz.hub.example.com"}, true)

	assert.NotEqual(t, before, after)
}
