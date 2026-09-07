package sessions

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

func TestCookieValue_Roundtrip(t *testing.T) {
	t.Parallel()

	key := signingKey("app-1", "salt")
	deadline := time.Now().Add(30 * time.Minute).Truncate(time.Second)

	sessionID, err := newSessionID()
	require.NoError(t, err)

	gotID, gotDeadline, err := decodeCookieValue(encodeCookieValue(sessionID, deadline, key), key)
	require.NoError(t, err)
	assert.Equal(t, sessionID, gotID)
	assert.Equal(t, deadline.Unix(), gotDeadline.Unix())
}

func TestDecodeCookieValue_DeadlineIsSigned(t *testing.T) {
	t.Parallel()

	key := signingKey("app-1", "salt")
	now := time.Now()

	sessionID, err := newSessionID()
	require.NoError(t, err)

	value := encodeCookieValue(sessionID, now.Add(time.Minute), key)
	_, signature, _ := strings.Cut(value[strings.LastIndexByte(value, '.'):], ".")

	// Re-stating the deadline while keeping the old signature must be rejected:
	// otherwise a client could hand itself an endless session.
	forged := sessionID + "." + strconv.FormatInt(now.Add(100*time.Hour).Unix(), 10) + "." + signature
	_, _, err = decodeCookieValue(forged, key)
	assert.Error(t, err)
}

func TestDecodeCookieValue_Invalid(t *testing.T) {
	t.Parallel()

	key := signingKey("app-1", "salt")

	sessionID, err := newSessionID()
	require.NoError(t, err)
	deadline := time.Now().Add(30 * time.Minute)
	valid := encodeCookieValue(sessionID, deadline, key)

	for name, value := range map[string]string{
		"empty":                 "",
		"no separator":          sessionID,
		"no deadline":           sessionID + "." + sign(sessionID, key),
		"empty signature":       sessionID + ".0.",
		"wrong signature":       sessionID + ".0.deadbeefdeadbeefdeadbeefdeadbeef",
		"deadline not a number": sessionID + ".soon." + sign(sessionID+".soon", key),
		"signed elsewhere":      encodeCookieValue(sessionID, deadline, signingKey("app-1", "other-salt")),
		"other app":             encodeCookieValue(sessionID, deadline, signingKey("app-2", "salt")),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeCookieValue(value, key)
			assert.Error(t, err)
		})
	}

	// Sanity check that the fixture the negative cases are derived from is valid.
	_, _, err = decodeCookieValue(valid, key)
	require.NoError(t, err)
}

func TestSessionStartFromID(t *testing.T) {
	t.Parallel()

	before := time.Now()
	sessionID, err := newSessionID()
	require.NoError(t, err)
	after := time.Now()

	startedAt, err := sessionStartFromID(sessionID)
	require.NoError(t, err)

	// The mint time is recovered from the UUIDv7 itself, which is what lets a
	// session survive a proxy restart without any stored state.
	assert.False(t, startedAt.Before(before.Truncate(time.Millisecond)))
	assert.False(t, startedAt.After(after.Add(time.Millisecond)))
}

func TestSessionStartFromID_NotV7(t *testing.T) {
	t.Parallel()

	// UUIDv4 carries no timestamp.
	_, err := sessionStartFromID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	assert.Error(t, err)
}

func TestReadCookie(t *testing.T) {
	t.Parallel()

	key := signingKey("app-1", "salt")
	sessionID, err := newSessionID()
	require.NoError(t, err)

	const maxSessionLength = 12 * time.Hour
	now := time.Now()

	request := func(t *testing.T, value string) *http.Request {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.local/", nil)
		require.NoError(t, err)
		if value != "" {
			req.AddCookie(&http.Cookie{Name: CookieName, Value: value})
		}
		return req
	}

	valid := encodeCookieValue(sessionID, now.Add(30*time.Minute), key)

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		_, found := readCookie(request(t, ""), key, now, maxSessionLength)
		assert.False(t, found)
	})

	t.Run("present", func(t *testing.T) {
		t.Parallel()
		state, found := readCookie(request(t, valid), key, now, maxSessionLength)
		assert.True(t, found)
		assert.Equal(t, sessionID, state.sessionID)
		assert.False(t, state.startedAt.IsZero())
		assert.Equal(t, now.Add(30*time.Minute).Unix(), state.deadline.Unix())
	})

	t.Run("forged", func(t *testing.T) {
		t.Parallel()
		_, found := readCookie(request(t, sessionID+".0.00000000000000000000000000000000"), key, now, maxSessionLength)
		assert.False(t, found)
	})

	t.Run("past its deadline", func(t *testing.T) {
		t.Parallel()
		// Max-Age is only a hint the client may ignore, so the deadline is
		// enforced here too.
		_, found := readCookie(request(t, valid), key, now.Add(31*time.Minute), maxSessionLength)
		assert.False(t, found)
	})

	t.Run("past the absolute cap", func(t *testing.T) {
		t.Parallel()
		// A deadline far in the future must not defeat the cap on how long one
		// session may run.
		far := encodeCookieValue(sessionID, now.Add(100*time.Hour), key)
		_, found := readCookie(request(t, far), key, now.Add(maxSessionLength+time.Minute), maxSessionLength)
		assert.False(t, found)
	})
}

func TestSigningKey_PerApp(t *testing.T) {
	t.Parallel()

	app1 := signingKey(api.AppID("app-1"), "salt")
	app2 := signingKey(api.AppID("app-2"), "salt")
	app1Again := signingKey(api.AppID("app-1"), "salt")

	// Distinct keys per app mean a cookie minted for one app cannot be replayed
	// as a valid session of another.
	assert.NotEqual(t, app1, app2)
	// Derivation must be stable, otherwise every proxy restart would invalidate
	// all live session cookies.
	assert.Equal(t, app1, app1Again)
}
