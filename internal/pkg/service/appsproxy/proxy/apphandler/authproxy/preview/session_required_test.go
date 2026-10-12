package preview_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
)

func TestIsFrameDocumentLoad(t *testing.T) {
	t.Parallel()
	html := "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
	cases := map[string]struct {
		dest, accept string
		want         bool
	}{
		"iframe":           {"iframe", html, true},
		"frame":            {"frame", html, true},
		"document":         {"document", html, false},
		"no-dest":          {"", html, false},
		"iframe-not-html":  {"iframe", "application/json", false},
		"iframe-no-accept": {"iframe", "", false},
	}
	for name, tc := range cases {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-123.hub.keboola.local/", nil)
		if tc.dest != "" {
			req.Header.Set("Sec-Fetch-Dest", tc.dest)
		}
		if tc.accept != "" {
			req.Header.Set("Accept", tc.accept)
		}
		assert.Equal(t, tc.want, preview.IsFrameDocumentLoad(req), name)
	}
}

func TestSessionRequiredCSP(t *testing.T) {
	t.Parallel()
	assert.Equal(t,
		"default-src 'none'; script-src 'nonce-abc'; form-action 'none'; base-uri 'none'; frame-ancestors 'none'",
		preview.SessionRequiredCSP("abc", nil))
	assert.Equal(t,
		"default-src 'none'; script-src 'nonce-abc'; form-action 'none'; base-uri 'none'; frame-ancestors https://a.example https://b.example",
		preview.SessionRequiredCSP("abc", []string{"https://a.example", "https://b.example"}))
}
