package session_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
)

const testOrigin = "https://my-app-123.hub.keboola.local"

const sessionKey = "0123456789abcdef0123456789abcdef0123456789abcdef"

var sessionStart = time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // test fixture

func newSessions(clock clockwork.Clock) *session.Manager {
	return session.NewManager(sessionKey, 4*time.Hour, 12*time.Hour, clock)
}

func unverifiedClaims(t *testing.T, raw string) *session.Claims {
	t.Helper()
	claims := &session.Claims{}
	_, _, err := jwt.NewParser().ParseUnverified(raw, claims)
	require.NoError(t, err)
	return claims
}

func TestSessions_IssueCookieAttributes(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	cookie, err := newSessions(clock).Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	assert.Equal(t, "__Host-kbc-app-preview-session", cookie.Name)
	assert.Equal(t, "/", cookie.Path)
	assert.Empty(t, cookie.Domain)
	assert.True(t, cookie.Secure)
	assert.True(t, cookie.HttpOnly)
	assert.Equal(t, http.SameSiteNoneMode, cookie.SameSite)
	assert.True(t, cookie.Partitioned)
	assert.Equal(t, sessionStart.Add(4*time.Hour), cookie.Expires)
	assert.Equal(t, int((4 * time.Hour).Seconds()), cookie.MaxAge)

	rec := httptest.NewRecorder()
	http.SetCookie(rec, cookie)
	header := rec.Header().Get("Set-Cookie")
	for _, part := range []string{"__Host-kbc-app-preview-session=", "Path=/", "Secure", "HttpOnly", "SameSite=None", "Partitioned"} {
		assert.Contains(t, header, part)
	}
	assert.NotContains(t, header, "Domain=")

	claims := unverifiedClaims(t, cookie.Value)
	assert.Equal(t, 1, claims.Ver)
	assert.Equal(t, "app-preview-session", claims.Purpose)
	assert.Equal(t, testOrigin, claims.Subject)
	assert.NotEmpty(t, claims.ID)
	assert.Equal(t, sessionStart.Unix(), claims.AuthTime)
	assert.Equal(t, "link-jti", claims.LinkJTI)
	assert.Equal(t, sessionStart, claims.IssuedAt.UTC())
	assert.Equal(t, sessionStart.Add(4*time.Hour), claims.ExpiresAt.UTC())
}

func TestSessions_IssueCookieMaxAgeWholeSeconds(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart.Add(500 * time.Millisecond))
	cookie, err := newSessions(clock).Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	assert.Equal(t, int((4 * time.Hour).Seconds()), cookie.MaxAge)
	assert.Equal(t, sessionStart.Add(4*time.Hour), cookie.Expires)
}

func TestSessions_IdleSlide(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	sessions := newSessions(clock)
	cookie, err := sessions.Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	clock.Advance(session.SlideInterval - time.Second)
	_, refresh, ok := sessions.Check(cookie.Value, testOrigin)
	require.True(t, ok)
	assert.Nil(t, refresh, "no slide before the slide interval")

	clock.Advance(time.Second)
	_, refresh, ok = sessions.Check(cookie.Value, testOrigin)
	require.True(t, ok)
	require.NotNil(t, refresh, "slide once the slide interval has passed")
	assert.Equal(t, sessionStart.Add(session.SlideInterval+4*time.Hour), refresh.Expires)
	claims := unverifiedClaims(t, refresh.Value)
	assert.Equal(t, sessionStart.Unix(), claims.AuthTime, "authTime never changes")
	assert.Equal(t, "link-jti", claims.LinkJTI)
	assert.NotEqual(t, unverifiedClaims(t, cookie.Value).ID, claims.ID)
}

func TestSessions_RealIdleTimeoutIsFourHours(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	sessions := newSessions(clock)
	cookie, err := sessions.Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	clock.Advance(time.Hour + 59*time.Minute)
	_, refresh, ok := sessions.Check(cookie.Value, testOrigin)
	require.True(t, ok)
	require.NotNil(t, refresh)
	assert.Equal(t, clock.Now().Add(4*time.Hour), refresh.Expires)

	clock.Advance(4*time.Hour - time.Second)
	_, _, ok = sessions.Check(refresh.Value, testOrigin)
	assert.True(t, ok, "valid until four hours after the last request")

	clock.Advance(time.Second)
	_, _, ok = sessions.Check(refresh.Value, testOrigin)
	assert.False(t, ok, "expired four hours after the last request")
}

func TestSessions_IdleExpiry(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	sessions := newSessions(clock)
	cookie, err := sessions.Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	clock.Advance(4*time.Hour + time.Second)
	_, _, ok := sessions.Check(cookie.Value, testOrigin)
	assert.False(t, ok)
}

func TestSessions_HardCap(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	sessions := newSessions(clock)
	cookie, err := sessions.Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	value := cookie.Value
	for elapsed := 2 * time.Hour; elapsed <= 8*time.Hour; elapsed += 2 * time.Hour {
		clock.Advance(2 * time.Hour)
		_, refresh, ok := sessions.Check(value, testOrigin)
		require.True(t, ok, elapsed.String())
		require.NotNil(t, refresh, elapsed.String())
		value = refresh.Value
	}
	assert.Equal(t, sessionStart.Add(12*time.Hour), unverifiedClaims(t, value).ExpiresAt.UTC(), "the 8h slide is clipped to the cap")

	clock.Advance(2 * time.Hour)
	_, refresh, ok := sessions.Check(value, testOrigin)
	require.True(t, ok)
	assert.Nil(t, refresh, "no slide when it would not move exp")

	clock.Advance(2*time.Hour + time.Second)
	_, _, ok = sessions.Check(value, testOrigin)
	assert.False(t, ok, "never valid past authTime + maxTTL")
}

func TestSessions_ToleratesReplicaClockSkew(t *testing.T) {
	t.Parallel()
	issuer := clockwork.NewFakeClockAt(sessionStart)
	cookie, err := newSessions(issuer).Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	checker := clockwork.NewFakeClockAt(sessionStart.Add(-time.Second))
	behind := session.NewManager(sessionKey, 4*time.Hour, 12*time.Hour, checker)
	_, _, ok := behind.Check(cookie.Value, testOrigin)
	assert.True(t, ok, "a cookie issued by a replica whose clock is slightly ahead must still be accepted")
}

func TestSessions_CapIsEnforcedOnCheck(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	cookie, err := newSessions(clock).Issue(testOrigin, "link-jti")
	require.NoError(t, err)

	shorter := session.NewManager(sessionKey, 4*time.Hour, time.Hour, clock)
	clock.Advance(time.Hour + time.Second)
	_, _, ok := shorter.Check(cookie.Value, testOrigin)
	assert.False(t, ok, "a lowered cap applies to cookies issued before")
}

func TestSessions_CapShorterThanIdle(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	cookie, err := session.NewManager(sessionKey, 4*time.Hour, time.Hour, clock).Issue(testOrigin, "j")
	require.NoError(t, err)
	assert.Equal(t, sessionStart.Add(time.Hour), cookie.Expires.UTC())
}

func TestSessions_RejectsForeignCookies(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClockAt(sessionStart)
	sessions := newSessions(clock)
	cookie, err := sessions.Issue(testOrigin, "j")
	require.NoError(t, err)

	_, _, ok := sessions.Check(cookie.Value, "https://other-999.hub.keboola.local")
	assert.False(t, ok, "wrong sub")

	_, _, ok = session.NewManager(strings.Repeat("x", 48), 4*time.Hour, 12*time.Hour, clock).Check(cookie.Value, testOrigin)
	assert.False(t, ok, "wrong key")

	none := jwt.NewWithClaims(jwt.SigningMethodNone, unverifiedClaims(t, cookie.Value))
	noneToken, err := none.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	_, _, ok = sessions.Check(noneToken, testOrigin)
	assert.False(t, ok, "alg none")

	other := unverifiedClaims(t, cookie.Value)
	other.Purpose = "other-purpose"
	forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256, other).SignedString([]byte(sessionKey))
	require.NoError(t, err)
	_, _, ok = sessions.Check(forged, testOrigin)
	assert.False(t, ok, "wrong purpose")

	noIat := unverifiedClaims(t, cookie.Value)
	noIat.IssuedAt = nil
	forgedNoIat, err := jwt.NewWithClaims(jwt.SigningMethodHS256, noIat).SignedString([]byte(sessionKey))
	require.NoError(t, err)
	_, _, ok = sessions.Check(forgedNoIat, testOrigin)
	assert.False(t, ok, "no iat")

	verTwo := unverifiedClaims(t, cookie.Value)
	verTwo.Ver = 2
	forgedVerTwo, err := jwt.NewWithClaims(jwt.SigningMethodHS256, verTwo).SignedString([]byte(sessionKey))
	require.NoError(t, err)
	_, _, ok = sessions.Check(forgedVerTwo, testOrigin)
	assert.False(t, ok, "ver 2")

	zeroAuthTime := unverifiedClaims(t, cookie.Value)
	zeroAuthTime.AuthTime = 0
	forgedZeroAuthTime, err := jwt.NewWithClaims(jwt.SigningMethodHS256, zeroAuthTime).SignedString([]byte(sessionKey))
	require.NoError(t, err)
	_, _, ok = sessions.Check(forgedZeroAuthTime, testOrigin)
	assert.False(t, ok, "authTime 0")

	_, _, ok = sessions.Check("", testOrigin)
	assert.False(t, ok, "empty")
}
