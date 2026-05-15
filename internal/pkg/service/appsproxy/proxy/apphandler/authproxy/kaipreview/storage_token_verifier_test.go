package kaipreview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageTokenVerifier_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/v2/storage/tokens/verify", r.URL.Path)
		assert.Equal(t, "test-token", r.Header.Get("X-StorageApi-Token"))
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"owner": map[string]any{"id": "proj-456", "name": "Test Project"},
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	v := NewHTTPStorageTokenVerifier(srv.URL, srv.Client())
	res, err := v.Verify(t.Context(), "test-token")
	require.NoError(t, err)
	assert.Equal(t, "proj-456", res.ProjectID)
}

func TestStorageTokenVerifier_Unauthorized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	v := NewHTTPStorageTokenVerifier(srv.URL, srv.Client())
	_, err := v.Verify(t.Context(), "bad-token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unauthorized")
}

func TestStorageTokenVerifier_NetworkError(t *testing.T) {
	t.Parallel()
	v := NewHTTPStorageTokenVerifier("http://127.0.0.1:1", http.DefaultClient) // port 1 = unreachable
	_, err := v.Verify(t.Context(), "token")
	require.Error(t, err)
}

func TestStorageTokenVerifier_MissingOwnerID(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"owner":{}}`))
	}))
	defer srv.Close()

	v := NewHTTPStorageTokenVerifier(srv.URL, srv.Client())
	_, err := v.Verify(t.Context(), "token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing owner.id")
}

func TestStorageTokenVerifier_ServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	v := NewHTTPStorageTokenVerifier(srv.URL, srv.Client())
	_, err := v.Verify(t.Context(), "token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestStorageTokenVerifier_MalformedJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	v := NewHTTPStorageTokenVerifier(srv.URL, srv.Client())
	_, err := v.Verify(t.Context(), "token")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode Storage token verify response")
}
