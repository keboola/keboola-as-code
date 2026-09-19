package apphandler

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
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
