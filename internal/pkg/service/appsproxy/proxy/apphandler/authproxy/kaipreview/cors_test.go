package kaipreview

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCORS_AllowedOrigin_Preflight(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	r := httptest.NewRequest(http.MethodOptions, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	r.Header.Set("Access-Control-Request-Headers", "X-StorageApi-Token, Content-Type")
	w := httptest.NewRecorder()

	handled := cors.HandlePreflight(w, r)
	assert.True(t, handled)
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
	assert.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "X-StorageApi-Token")
}

func TestCORS_DisallowedOrigin_Preflight(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	r := httptest.NewRequest(http.MethodOptions, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	w := httptest.NewRecorder()

	handled := cors.HandlePreflight(w, r)
	assert.True(t, handled)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORS_WriteResponseHeaders(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})

	w := httptest.NewRecorder()
	cors.WriteResponseHeaders(w, "https://connection.keboola.com")
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))
}

func TestCORS_NonPreflightPassesThrough(t *testing.T) {
	t.Parallel()
	cors := NewCORS([]string{"https://connection.keboola.com"})
	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	w := httptest.NewRecorder()
	handled := cors.HandlePreflight(w, r)
	assert.False(t, handled)
}
