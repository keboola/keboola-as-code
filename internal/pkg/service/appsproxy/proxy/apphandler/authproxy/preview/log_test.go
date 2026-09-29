package preview_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
)

func TestSanitizeClaimForLog(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "2026-09", preview.SanitizeClaimForLog("2026-09"))
	assert.Equal(t, "a_b__c", preview.SanitizeClaimForLog("a b\n\"c"))
	assert.Len(t, preview.SanitizeClaimForLog(string(make([]byte, 500))), 64+3)
}
