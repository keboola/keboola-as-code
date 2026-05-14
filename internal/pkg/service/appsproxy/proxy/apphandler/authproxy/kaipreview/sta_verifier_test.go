package kaipreview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSTAVerifier_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/v2/storage/tokens/verify", r.URL.Path)
		assert.Equal(t, "test-token", r.Header.Get("X-StorageApi-Token"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"owner": map[string]any{"id": "proj-456", "name": "Test Project"},
		})
	}))
	defer srv.Close()

	v := NewSTAVerifier(srv.URL, srv.Client())
	res, err := v.Verify(context.Background(), "test-token")
	require.NoError(t, err)
	assert.Equal(t, "proj-456", res.ProjectID)
}

func TestSTAVerifier_Unauthorized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	v := NewSTAVerifier(srv.URL, srv.Client())
	_, err := v.Verify(context.Background(), "bad-token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unauthorized")
}

func TestSTAVerifier_NetworkError(t *testing.T) {
	t.Parallel()
	v := NewSTAVerifier("http://127.0.0.1:1", http.DefaultClient) // port 1 = unreachable
	_, err := v.Verify(context.Background(), "token")
	require.Error(t, err)
}
