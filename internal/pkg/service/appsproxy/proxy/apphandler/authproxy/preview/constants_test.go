package preview_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
)

func TestTimingConstants(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 4*time.Hour, preview.SessionIdleTTL)
	assert.Equal(t, 12*time.Hour, preview.SessionMaxTTL)
	assert.Equal(t, 5*time.Minute, preview.SessionSlideInterval)
	assert.Equal(t, 30*time.Second, preview.ClockSkew)
}
