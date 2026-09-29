package preview_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
)

func TestIsSameOriginRedeem(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{"same-origin": true, "same-site": false, "cross-site": false, "none": false, "": false}
	for value, want := range cases {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "https://my-app-123.hub.keboola.local/_proxy/preview", nil)
		if value != "" {
			req.Header.Set("Sec-Fetch-Site", value)
		}
		req.Header.Set("Origin", "null")
		assert.Equal(t, want, preview.IsSameOriginRedeem(req), value)
	}
}

func TestLandingCSP(t *testing.T) {
	t.Parallel()
	assert.Equal(t,
		"default-src 'none'; script-src 'nonce-abc'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
		preview.LandingCSP("abc", nil))
	assert.Equal(t,
		"default-src 'none'; script-src 'nonce-abc'; form-action 'self'; base-uri 'none'; frame-ancestors https://connection.keboola.com https://connection.eu-central-1.keboola.com",
		preview.LandingCSP("abc", []string{"https://connection.keboola.com", "https://connection.eu-central-1.keboola.com"}))
}
