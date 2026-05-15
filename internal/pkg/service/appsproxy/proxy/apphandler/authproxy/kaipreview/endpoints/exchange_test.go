package endpoints

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
)

func newTestExchangeHandler(devMode bool) *ExchangeHandler {
	return NewExchangeHandler(ExchangeDeps{
		Clock:        clockwork.NewFakeClock(),
		DevMode:      &stubDevModeChecker{devMode: devMode},
		HandshakeKey: testHandshakeKey,
		SessionKey:   testSessionKey,
		SessionTTL:   4 * time.Hour,
		AppID:        "app-123",
		AppProjectID: "proj-456",
	})
}

func mintForTest(t *testing.T, appID, projectID string) string {
	t.Helper()
	clock := clockwork.NewFakeClock()
	jwt, err := kaipreview.MintHandshakeJWT(testHandshakeKey, clock, appID, projectID)
	require.NoError(t, err)
	return jwt
}

func TestExchangeHandler_Success(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(true)
	jwt := mintForTest(t, "app-123", "proj-456")

	body, err := json.Marshal(map[string]string{"token": jwt})
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)

	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	assert.Equal(t, kaipreview.SessionCookieName, cookies[0].Name)
	assert.True(t, cookies[0].Partitioned)
}

func TestExchangeHandler_InvalidJWT(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(true)

	body, err := json.Marshal(map[string]string{"token": "not-a-jwt"})
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", bytes.NewReader(body))
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestExchangeHandler_AppIDMismatch(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(true)
	jwt := mintForTest(t, "different-app", "proj-456")

	body, err := json.Marshal(map[string]string{"token": jwt})
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", bytes.NewReader(body))
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestExchangeHandler_ProjectMismatch(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(true)
	jwt := mintForTest(t, "app-123", "different-project")

	body, err := json.Marshal(map[string]string{"token": jwt})
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", bytes.NewReader(body))
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestExchangeHandler_DevModeOff(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(false)
	jwt := mintForTest(t, "app-123", "proj-456")

	body, err := json.Marshal(map[string]string{"token": jwt})
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", bytes.NewReader(body))
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestExchangeHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(true)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_proxy/kai-preview/exchange", nil)
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestExchangeHandler_EmptyBody(t *testing.T) {
	t.Parallel()
	h := newTestExchangeHandler(true)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/exchange", bytes.NewReader([]byte("{}")))
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
