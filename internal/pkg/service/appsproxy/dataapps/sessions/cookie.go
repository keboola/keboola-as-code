package sessions

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CookieName holds the session id. Distinct from the oauth2-proxy session
// cookie: this one also exists for apps with no authentication at all.
const CookieName = "_kbc_dasid"

// keyPrefix namespaces the cookie signing key away from other keys derived
// from the same salt.
const keyPrefix = "data-app-session/"

// signatureLen is the number of HMAC bytes kept in the cookie, hex-encoded.
// 16 bytes (128 bits) is far beyond what forging a session id is worth here.
const signatureLen = 16

// newSessionID returns a fresh UUIDv7. Version 7 is used deliberately: its
// timestamp prefix lets sessionStart be recovered from the id alone, so the
// proxy does not need to remember when a session began — which matters because
// the proxy runs multiple replicas and restarts freely.
func newSessionID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// sessionStartFromID recovers the mint time from a UUIDv7 session id.
func sessionStartFromID(id string) (time.Time, error) {
	parsed, err := uuid.FromString(id)
	if err != nil {
		return time.Time{}, err
	}
	ts, err := uuid.TimestampFromV7(parsed)
	if err != nil {
		return time.Time{}, err
	}
	return ts.Time()
}

// signingKey derives a per-app HMAC key. Per-app keys mean a cookie minted for
// one app cannot be replayed as a valid session of another.
//
// The leading keyPrefix keeps this out of the oauth2-proxy cookie secret's key
// space, which is SHA256(appID + "/" + providerID + "/" + salt): without it, an
// app configuring a provider with the id "sessions" would derive the very same
// key for both.
func signingKey(appID api.AppID, salt string) []byte {
	var b strings.Builder
	b.WriteString(keyPrefix)
	b.WriteString(appID.String())
	b.WriteByte('/')
	b.WriteString(salt)

	h := sha256.New()
	h.Write([]byte(b.String()))
	return h.Sum(nil)
}

func sign(payload string, key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil)[:signatureLen])
}

// encodeCookieValue returns "<sessionID>.<deadlineUnix>.<signature>".
//
// The deadline is inside the signed payload, not just in the cookie's Max-Age.
// Max-Age is a hint the client may ignore; this is what the proxy enforces, and
// signing it means only the proxy can move it.
func encodeCookieValue(sessionID string, deadline time.Time, key []byte) string {
	payload := cookiePayload(sessionID, deadline)

	var b strings.Builder
	b.WriteString(payload)
	b.WriteByte('.')
	b.WriteString(sign(payload, key))
	return b.String()
}

func cookiePayload(sessionID string, deadline time.Time) string {
	var b strings.Builder
	b.WriteString(sessionID)
	b.WriteByte('.')
	b.WriteString(strconv.FormatInt(deadline.Unix(), 10))
	return b.String()
}

// decodeCookieValue verifies the signature and returns the session id and the
// deadline the proxy issued. An unsigned or wrongly signed value is rejected,
// so a client cannot invent session ids and have them written to the project
// table, nor extend a session of its own accord.
func decodeCookieValue(value string, key []byte) (string, time.Time, error) {
	payload, signature, found := strings.LastIndexByte(value, '.'), "", false
	if payload >= 0 {
		signature, found = value[payload+1:], true
	}
	if !found {
		return "", time.Time{}, errors.New("malformed session cookie")
	}
	signed := value[:payload]

	if !hmac.Equal([]byte(signature), []byte(sign(signed, key))) {
		return "", time.Time{}, errors.New("invalid session cookie signature")
	}

	sessionID, deadlineStr, found := strings.Cut(signed, ".")
	if !found {
		return "", time.Time{}, errors.New("malformed session cookie")
	}
	deadlineUnix, err := strconv.ParseInt(deadlineStr, 10, 64)
	if err != nil {
		return "", time.Time{}, errors.New("malformed session cookie deadline")
	}

	return sessionID, time.Unix(deadlineUnix, 0), nil
}

// cookieState is what a valid session cookie carries.
type cookieState struct {
	sessionID string
	startedAt time.Time // decoded from the UUIDv7, so the client cannot move it
	deadline  time.Time // issued and signed by the proxy
}

// readCookie returns the state of the session cookie on the request, if it
// carries a valid one that is neither past its deadline nor past the absolute
// cap on a session's length.
//
// Both checks happen here rather than being left to the browser: Max-Age is
// only a hint a client may ignore, so without them a replayed cookie could
// manufacture a session of any length with a start time of its choosing.
func readCookie(req *http.Request, key []byte, now time.Time, maxSessionLength time.Duration) (cookieState, bool) {
	cookie, err := req.Cookie(CookieName)
	if err != nil {
		return cookieState{}, false
	}

	sessionID, deadline, err := decodeCookieValue(cookie.Value, key)
	if err != nil {
		return cookieState{}, false
	}
	if now.After(deadline) {
		return cookieState{}, false
	}

	startedAt, err := sessionStartFromID(sessionID)
	if err != nil {
		return cookieState{}, false
	}
	if now.Sub(startedAt) > maxSessionLength {
		return cookieState{}, false
	}

	return cookieState{sessionID: sessionID, startedAt: startedAt, deadline: deadline}, true
}

// setCookie writes the session cookie with the given deadline. Max-Age is
// derived from that deadline, so the browser drops the cookie at roughly the
// moment the proxy would start rejecting it.
//
// SameSite=Lax, not Strict: the request that lands back on the app after an
// OAuth redirect is a cross-site top-level navigation. Under Strict the cookie
// would not be sent on it, and the proxy would mint a second session
// immediately after every login.
func setCookie(rw http.ResponseWriter, app api.AppConfig, publicURL *url.URL, sessionID string, deadline time.Time, now time.Time, key []byte) {
	maxAge := int(deadline.Sub(now).Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	http.SetCookie(rw, newCookie(app, publicURL, encodeCookieValue(sessionID, deadline, key), maxAge))
}

// clearCookie expires the session cookie, so the next visitor on this browser
// starts a session of their own instead of continuing this one.
func clearCookie(rw http.ResponseWriter, app api.AppConfig, publicURL *url.URL) {
	http.SetCookie(rw, newCookie(app, publicURL, "", -1))
}

func newCookie(app api.AppConfig, publicURL *url.URL, value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     CookieName,
		Value:    value,
		Path:     "/",
		Domain:   app.CookieDomain(publicURL),
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	}
}
