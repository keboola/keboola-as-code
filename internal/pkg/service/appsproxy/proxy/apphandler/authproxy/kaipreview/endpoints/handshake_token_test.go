package endpoints

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
)

const (
	testHandshakeKey = "test-handshake-key-must-be-long-enough"
	testSessionKey   = "test-session-key-also-long-enough"
)

type stubStorageTokenVerifier struct {
	projectID string
	err       error
}

func (s *stubStorageTokenVerifier) Verify(_ context.Context, _ string) (*kaipreview.StorageTokenVerifyResult, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &kaipreview.StorageTokenVerifyResult{ProjectID: s.projectID}, nil
}

type stubDevModeChecker struct{ devMode bool }

func (s *stubDevModeChecker) IsDevMode(_ context.Context, _ string) bool { return s.devMode }

var errStubUnauth = &stubError{msg: "unauthorized"}

type stubError struct{ msg string }

func (e *stubError) Error() string { return e.msg }

func newTestHandshakeHandler(tokenValid bool, storageTokenProject string, devMode bool) *HandshakeTokenHandler {
	var verifier kaipreview.StorageTokenVerifier
	if tokenValid {
		verifier = &stubStorageTokenVerifier{projectID: storageTokenProject}
	} else {
		verifier = &stubStorageTokenVerifier{err: errStubUnauth}
	}
	return NewHandshakeTokenHandler(HandshakeTokenDeps{
		Logger:               log.NewNopLogger(),
		Clock:                clockwork.NewFakeClock(),
		StorageTokenVerifier: verifier,
		DevMode:              &stubDevModeChecker{devMode: devMode},
		CORS:                 kaipreview.NewCORS([]string{"https://connection.keboola.com"}),
		HandshakeKey:         testHandshakeKey,
		SessionTTL:           4 * time.Hour,
		AppID:                "app-123",
		AppProjectID:         "proj-456",
	})
}

func TestHandshakeTokenHandler_Success(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "proj-456", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
	// Mint uses header auth — ACAC must NOT be set (C2: no credentials on mint preflight).
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"),
		"mint response must NOT set Access-Control-Allow-Credentials (header-auth endpoint)")

	// I2: response shape must be {token, jti, sessionTtlSeconds}.
	var body struct {
		Token             string `json:"token"`
		JTI               string `json:"jti"`
		SessionTTLSeconds int    `json:"sessionTtlSeconds"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.NotEmpty(t, body.Token)
	assert.NotEmpty(t, body.JTI, "jti must be present so SPA can correlate logs")
	assert.Equal(t, int((4 * time.Hour).Seconds()), body.SessionTTLSeconds,
		"sessionTtlSeconds must match configured SessionTTL so SPA can derive heartbeat cadence")
}

func TestHandshakeTokenHandler_PreflightOptions(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "proj-456", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandshakeTokenHandler_MissingSTAHeader(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "proj-456", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers so SPA can read status")
}

func TestHandshakeTokenHandler_STAInvalid(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(false, "", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "bad-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers so SPA can read status")
}

func TestHandshakeTokenHandler_WrongProject(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "different-project", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid-but-wrong-project")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers so SPA can read status")
}

func TestHandshakeTokenHandler_AppNotInDevMode(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "proj-456", false)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, w.Code, "must look like the endpoint doesn't exist when app isn't in dev mode")
}

func TestHandshakeTokenHandler_DisallowedOrigin(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "proj-456", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	r.Header.Set("X-StorageApi-Token", "valid-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"),
		"disallowed origins must NOT receive CORS headers")
}

func TestHandshakeTokenHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(true, "proj-456", true)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandshakeTokenHandler_NoTokenInErrorBody(t *testing.T) {
	t.Parallel()
	h := newTestHandshakeHandler(false, "", true)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "secret-token-do-not-leak")
	w := httptest.NewRecorder()
	_ = h.ServeHTTPOrError(w, r)
	assert.NotContains(t, w.Body.String(), "secret-token-do-not-leak")
}
