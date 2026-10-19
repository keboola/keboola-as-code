package k8sapp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
)

// client-go's defaults are 5 QPS and burst 10, meant for a small controller.
// One client here is shared by the informers, the wake path, the suspend path
// and every activity write, and a queued write that times out leaves the shared
// record stale, which is how a load peak turns into an early suspend.
func TestRateLimits_AreRaisedAboveTheClientGoDefaults(t *testing.T) {
	t.Parallel()

	cfg := withRateLimits(&rest.Config{})

	assert.Greater(t, cfg.QPS, rest.DefaultQPS)
	assert.Greater(t, cfg.Burst, rest.DefaultBurst)
	assert.Greater(t, cfg.Burst, int(cfg.QPS), "a burst below the steady rate cannot absorb one")
}
