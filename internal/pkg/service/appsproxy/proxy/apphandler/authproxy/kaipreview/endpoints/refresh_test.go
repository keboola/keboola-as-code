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

func newTestRefreshHandler(devMode bool) (*RefreshHandler, *clockwork.FakeClock) {
	clock := clockwork.NewFakeClock()
	return NewRefreshHandler(RefreshDeps{
		Logger:       log.NewNopLogger(),
		Clock:        clock,
		DevMode:      &stubDevModeChecker{devMode: devMode},
		SessionKey:   testSessionKey,
		SessionTTL:   4 * time.Hour,
		CORS:         kaipreview.NewCORS([]string{"https://connection.keboola.com"}),
		AppID:        "app-123",
		AppProjectID: "proj-456",
	}), clock
}

func TestRefreshHandler_Success(t *testing.T) {
	t.Parallel()
	h, clock := newTestRefreshHandler(true)
	jwt, err := kaipreview.MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.AddCookie(&http.Cookie{Name: kaipreview.SessionCookieName, Value: jwt})
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))

	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	assert.Equal(t, kaipreview.SessionCookieName, cookies[0].Name)
	assert.NotEqual(t, jwt, cookies[0].Value, "must be a fresh JWT, not the old one")
}

func TestRefreshHandler_PreflightOptions(t *testing.T) {
	t.Parallel()
	h, _ := newTestRefreshHandler(true)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	w := httptest.NewRecorder()
	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestRefreshHandler_MissingCookie(t *testing.T) {
	t.Parallel()
	h, _ := newTestRefreshHandler(true)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"),
		"auth-failure responses to allowed origins must include CORS headers")
}

func TestRefreshHandler_ExpiredCookie(t *testing.T) {
	t.Parallel()
	h, clock := newTestRefreshHandler(true)
	jwt, err := kaipreview.MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)
	clock.Advance(5 * time.Hour)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.AddCookie(&http.Cookie{Name: kaipreview.SessionCookieName, Value: jwt})
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, "https://connection.keboola.com", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestRefreshHandler_DevModeOff(t *testing.T) {
	t.Parallel()
	h, clock := newTestRefreshHandler(false)
	jwt, err := kaipreview.MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.AddCookie(&http.Cookie{Name: kaipreview.SessionCookieName, Value: jwt})
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRefreshHandler_ScopeMismatch(t *testing.T) {
	t.Parallel()
	h, clock := newTestRefreshHandler(true)
	jwt, err := kaipreview.MintSessionJWT(testSessionKey, clock, "different-app", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	r.AddCookie(&http.Cookie{Name: kaipreview.SessionCookieName, Value: jwt})
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestRefreshHandler_DisallowedOrigin(t *testing.T) {
	t.Parallel()
	h, clock := newTestRefreshHandler(true)
	jwt, err := kaipreview.MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	r.AddCookie(&http.Cookie{Name: kaipreview.SessionCookieName, Value: jwt})
	w := httptest.NewRecorder()

	err = h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func TestRefreshHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h, _ := newTestRefreshHandler(true)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_proxy/kai-preview/refresh", nil)
	r.Header.Set("Origin", "https://connection.keboola.com")
	w := httptest.NewRecorder()

	err := h.ServeHTTPOrError(w, r)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
