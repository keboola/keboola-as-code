package endpoints

import (
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

func newTestCompositeHandler() *Handler {
	return NewHandler(HandlerDeps{
		Logger:               log.NewNopLogger(),
		Clock:                clockwork.NewFakeClock(),
		StorageTokenVerifier: &stubStorageTokenVerifier{projectID: "proj-456"},
		DevMode:              &stubDevModeChecker{devMode: true},
		CORS:                  kaipreview.NewCORS([]string{"https://connection.keboola.com"}),
		HandshakeKey:          testHandshakeKey,
		SessionKey:            testSessionKey,
		SessionTTL:            4 * time.Hour,
		AllowedFrameAncestors: []string{"https://connection.keboola.com"},
		AppID:                 "app-123",
		AppProjectID:          "proj-456",
	})
}

func TestCompositeHandler_RoutesHandshakeToken(t *testing.T) {
	t.Parallel()
	h := newTestCompositeHandler()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/handshake-token", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("X-StorageApi-Token", "valid")
	w := httptest.NewRecorder()
	require.NoError(t, h.ServeHTTPOrError(w, r))
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestCompositeHandler_RoutesBootstrap(t *testing.T) {
	t.Parallel()
	h := newTestCompositeHandler()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_proxy/kai-preview/bootstrap", nil)
	w := httptest.NewRecorder()
	require.NoError(t, h.ServeHTTPOrError(w, r))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
}

func TestCompositeHandler_RoutesExchange(t *testing.T) {
	t.Parallel()
	h := newTestCompositeHandler()
	// Empty body — exchange should return 400 (handler reached, valid path)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", nil)
	w := httptest.NewRecorder()
	require.NoError(t, h.ServeHTTPOrError(w, r))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCompositeHandler_RoutesRefresh(t *testing.T) {
	t.Parallel()
	h := newTestCompositeHandler()
	// No cookie — refresh should return 401 (handler reached, valid path)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()
	require.NoError(t, h.ServeHTTPOrError(w, r))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestCompositeHandler_UnknownSubpath404(t *testing.T) {
	t.Parallel()
	h := newTestCompositeHandler()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_proxy/kai-preview/does-not-exist", nil)
	w := httptest.NewRecorder()
	require.NoError(t, h.ServeHTTPOrError(w, r))
	assert.Equal(t, http.StatusNotFound, w.Code)
}
