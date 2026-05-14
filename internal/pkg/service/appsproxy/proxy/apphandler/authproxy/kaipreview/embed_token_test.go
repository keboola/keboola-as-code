package kaipreview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSTAVerifier struct {
	projectID string
	err       error
}

func (s *stubSTAVerifier) Verify(_ context.Context, _ string) (*STAVerifyResult, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &STAVerifyResult{ProjectID: s.projectID}, nil
}

type stubDevModeChecker struct{ devMode bool }

func (s *stubDevModeChecker) IsDevMode(_ context.Context, _ string) bool { return s.devMode }

var errStubUnauth = &stubErr{msg: "unauthorized"}

type stubErr struct{ msg string }

func (e *stubErr) Error() string { return e.msg }

func newTestEmbedHandler(staOK bool, staProject string, devMode bool) *EmbedTokenHandler {
	var sta STATokenVerifier
	if staOK {
		sta = &stubSTAVerifier{projectID: staProject}
	} else {
		sta = &stubSTAVerifier{err: errStubUnauth}
	}
	return NewEmbedTokenHandler(EmbedTokenDeps{
		Clock:        clockwork.NewFakeClock(),
		STA:          sta,
		DevMode:      &stubDevModeChecker{devMode: devMode},
		CORS:         NewCORS([]string{"https://connection.keboola.com"}),
		HandshakeKey: testHandshakeKey,
		AppID:        "app-123",
		AppProjectID: "proj-456",
	})
}

func TestEmbedTokenHandler_Success(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "proj-456", true)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))

	var body struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.NotEmpty(t, body.Token)
}

func TestEmbedTokenHandler_PreflightOptions(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "proj-456", true)

	r := httptest.NewRequest(http.MethodOptions, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestEmbedTokenHandler_MissingSTAHeader(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "proj-456", true)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers so SPA can read status")
}

func TestEmbedTokenHandler_STAInvalid(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(false, "", true)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "bad-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers so SPA can read status")
}

func TestEmbedTokenHandler_WrongProject(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "different-project", true)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid-but-wrong-project")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers so SPA can read status")
}

func TestEmbedTokenHandler_AppNotInDevMode(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "proj-456", false)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, w.Code, "must look like the endpoint doesn't exist when app isn't in dev mode")
}

func TestEmbedTokenHandler_DisallowedOrigin(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "proj-456", true)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	r.Header.Set("X-StorageApi-Token", "valid-token")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"),
		"disallowed origins must NOT receive CORS headers")
}

func TestEmbedTokenHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(true, "proj-456", true)
	r := httptest.NewRequest(http.MethodGet, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestEmbedTokenHandler_NoTokenInErrorBody(t *testing.T) {
	t.Parallel()
	h := newTestEmbedHandler(false, "", true)

	r := httptest.NewRequest(http.MethodPost, "/_proxy/kai-preview/embed-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "secret-token-do-not-leak")
	w := httptest.NewRecorder()
	_ = h.ServeHTTPOrError(w, r)
	assert.False(t, strings.Contains(w.Body.String(), "secret-token-do-not-leak"))
}
