package kaipreview

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCORS_AllowedOrigin_Preflight_WithCredentials(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	r := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	r.Header.Set("Access-Control-Request-Headers", "Content-Type")
	w := httptest.NewRecorder()

	handled := cors.HandlePreflight(w, r, true)
	assert.True(t, handled)
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORS_AllowedOrigin_Preflight_NoCredentials(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	r := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	r.Header.Set("Access-Control-Request-Headers", "X-StorageApi-Token, Content-Type")
	w := httptest.NewRecorder()

	// Mint endpoint: header auth, no ACAC.
	handled := cors.HandlePreflight(w, r, false)
	assert.True(t, handled)
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"), "mint preflight must NOT emit ACAC")
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "X-StorageApi-Token")
}

func TestCORS_DisallowedOrigin_Preflight(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	r := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	w := httptest.NewRecorder()

	handled := cors.HandlePreflight(w, r, false)
	assert.True(t, handled)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORS_WriteResponseHeaders_WithCredentials(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	w := httptest.NewRecorder()
	cors.WriteResponseHeaders(w, "https://connection.keboola.com", true)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORS_WriteResponseHeaders_NoCredentials(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	w := httptest.NewRecorder()
	cors.WriteResponseHeaders(w, "https://connection.keboola.com", false)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"), "must NOT emit ACAC when withCredentials=false")
}

func TestCORS_NonPreflightPassesThrough(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	w := httptest.NewRecorder()
	handled := cors.HandlePreflight(w, r, false)
	assert.False(t, handled)
}
