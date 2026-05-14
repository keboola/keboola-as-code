package kaipreview

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetSessionCookie_Attributes(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	clock := clockwork.NewFakeClock()
	ttl := 4 * time.Hour

	jwt, err := MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", ttl)
	require.NoError(t, err)

	SetSessionCookie(w, jwt, ttl)

	resp := w.Result()
	cookies := resp.Cookies()
	require.Len(t, cookies, 1)
	c := cookies[0]

	assert.Equal(t, SessionCookieName, c.Name)
	assert.Equal(t, jwt, c.Value)
	assert.Equal(t, "/", c.Path)
	assert.True(t, c.Secure)
	assert.True(t, c.HttpOnly)
	assert.Equal(t, http.SameSiteNoneMode, c.SameSite)
	assert.True(t, c.Partitioned)
	assert.Empty(t, c.Domain, "must be host-only — no Domain attribute")
	assert.Equal(t, int(ttl.Seconds()), c.MaxAge)
}

func TestReadSessionCookie_Present(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: SessionCookieName, Value: "the-jwt"})
	got := ReadSessionCookie(r)
	assert.Equal(t, "the-jwt", got)
}

func TestReadSessionCookie_Missing(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	got := ReadSessionCookie(r)
	assert.Empty(t, got)
}

func TestSetSessionCookie_NonPositiveTTL_ClearsInstead(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	SetSessionCookie(w, "ignored-jwt", 0)
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	assert.Equal(t, -1, cookies[0].MaxAge, "ttl=0 must invalidate, not create a session cookie")
	assert.Empty(t, cookies[0].Value)
}

func TestClearSessionCookie_Attributes(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ClearSessionCookie(w)
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1)
	c := cookies[0]
	assert.Equal(t, SessionCookieName, c.Name)
	assert.Empty(t, c.Value)
	assert.Equal(t, -1, c.MaxAge, "clear cookie must use MaxAge=-1")
	assert.True(t, c.Secure)
	assert.True(t, c.HttpOnly)
	assert.True(t, c.Partitioned)
	assert.Equal(t, http.SameSiteNoneMode, c.SameSite)
}

func TestValidateSessionCookie_Valid(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()
	jwt, err := MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)

	r := httptest.NewRequest("GET", "/anything", nil)
	r.AddCookie(&http.Cookie{Name: SessionCookieName, Value: jwt})

	claims, ok := ValidateSessionCookie(r, testSessionKey, clock, "app-123", "proj-456")
	assert.True(t, ok)
	require.NotNil(t, claims)
	assert.Equal(t, "app-123", claims.AppID)
}

func TestValidateSessionCookie_Missing(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()
	r := httptest.NewRequest("GET", "/anything", nil)
	_, ok := ValidateSessionCookie(r, testSessionKey, clock, "app-123", "proj-456")
	assert.False(t, ok)
}

func TestValidateSessionCookie_AppMismatch(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()
	jwt, err := MintSessionJWT(testSessionKey, clock, "different-app", "proj-456", 4*time.Hour)
	require.NoError(t, err)
	r := httptest.NewRequest("GET", "/anything", nil)
	r.AddCookie(&http.Cookie{Name: SessionCookieName, Value: jwt})

	_, ok := ValidateSessionCookie(r, testSessionKey, clock, "app-123", "proj-456")
	assert.False(t, ok)
}

func TestValidateSessionCookie_Expired(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()
	jwt, err := MintSessionJWT(testSessionKey, clock, "app-123", "proj-456", 4*time.Hour)
	require.NoError(t, err)
	clock.Advance(5 * time.Hour)
	r := httptest.NewRequest("GET", "/anything", nil)
	r.AddCookie(&http.Cookie{Name: SessionCookieName, Value: jwt})

	_, ok := ValidateSessionCookie(r, testSessionKey, clock, "app-123", "proj-456")
	assert.False(t, ok)
}

func TestValidateSessionCookie_ProjectMismatch(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()
	jwt, err := MintSessionJWT(testSessionKey, clock, "app-123", "different-project", 4*time.Hour)
	require.NoError(t, err)
	r := httptest.NewRequest("GET", "/anything", nil)
	r.AddCookie(&http.Cookie{Name: SessionCookieName, Value: jwt})

	_, ok := ValidateSessionCookie(r, testSessionKey, clock, "app-123", "proj-456")
	assert.False(t, ok)
}
