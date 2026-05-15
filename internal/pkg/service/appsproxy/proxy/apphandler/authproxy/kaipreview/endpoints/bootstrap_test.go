package endpoints

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapHandler_ServesHTMLWithCSP(t *testing.T) {
	t.Parallel()
	h := NewBootstrapHandler([]string{"https://connection.keboola.com"}, &stubDevModeChecker{devMode: true}, "app-123")
	r := httptest.NewRequestWithContext(t.Context(), "GET", "/_proxy/kai-preview/bootstrap", nil)
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, w.Header().Get("Content-Security-Policy"), "frame-ancestors https://connection.keboola.com")

	body := w.Body.String()
	assert.Contains(t, body, "postMessage", "shim must postMessage")
	assert.Contains(t, body, "kai-preview-ready", "shim must send 'kai-preview-ready' to parent")
	assert.Contains(t, body, "/_proxy/kai-preview/exchange", "shim must POST to exchange")
	assert.Contains(t, body, "https://connection.keboola.com", "shim must restrict postMessage targetOrigin")
	assert.NotContains(t, body, `targetOrigin: "*"`, "shim must NEVER postMessage with wildcard targetOrigin")
	assert.NotContains(t, strings.ToLower(body), "innerhtml", "shim must not write user-supplied data via innerHTML")
}

func TestBootstrapHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := NewBootstrapHandler([]string{"https://connection.keboola.com"}, &stubDevModeChecker{devMode: true}, "app-123")
	r := httptest.NewRequestWithContext(t.Context(), "POST", "/_proxy/kai-preview/bootstrap", nil)
	w := httptest.NewRecorder()
	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, 405, w.Code)
}

func TestBootstrapHandler_NonDevModeReturns404(t *testing.T) {
	t.Parallel()
	h := NewBootstrapHandler([]string{"https://connection.keboola.com"}, &stubDevModeChecker{devMode: false}, "app-123")
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_proxy/kai-preview/bootstrap", nil)
	w := httptest.NewRecorder()
	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, w.Code)
}
