package preview_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
)

func TestTimingConstants(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 30*time.Second, preview.ClockSkew)
}
