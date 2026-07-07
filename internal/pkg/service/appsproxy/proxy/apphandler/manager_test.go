package apphandler

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
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
