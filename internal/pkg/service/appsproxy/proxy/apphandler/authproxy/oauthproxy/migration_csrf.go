package oauthproxy

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/cookies"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
)

const (
	cookieStoreCSRFName = CookieName + "_csrf"
	redisStoreCSRFName  = redisSessionCookieName + "_csrf"
)

// renameCSRF keeps, for the callback, only a CSRF cookie that belongs to the login in its state, under the
// redis-store name: the redis-store cookie if it belongs, else a valid cookie-store cookie re-signed, else none.
// The CSRF payload is encrypted with the secret only.
func (m *cookieStoreMigration) renameCSRF(req *http.Request) {
	if req.URL.Path != m.callbackPath {
		return
	}
	nonce, ok := m.stateNonce(req)
	if !ok || m.csrfMatches(req, redisStoreCSRFName, nonce) {
		return
	}
	var signed string
	if m.csrfMatches(req, cookieStoreCSRFName, nonce) {
		old, _ := req.Cookie(cookieStoreCSRFName)
		signed, _ = m.resignCSRF(old, redisStoreCSRFName)
	}
	replaceRequestCookie(req, redisStoreCSRFName, signed)
}

func (m *cookieStoreMigration) stateNonce(req *http.Request) (string, bool) {
	state := req.URL.Query().Get("state")
	if m.proxyConfig.EncodeState {
		decoded, err := base64.RawURLEncoding.DecodeString(state)
		if err != nil {
			return "", false
		}
		state = string(decoded)
	}
	nonce, _, ok := strings.Cut(state, ":")
	return nonce, ok
}

func (m *cookieStoreMigration) csrfMatches(req *http.Request, name, nonce string) bool {
	csrf, err := cookies.LoadCSRFCookie(req, name, &m.proxyConfig.Cookie)
	return err == nil && csrf.CheckOAuthState(nonce)
}

// replaceRequestCookie removes all request cookies with the name and adds one with the value, if the value is not empty.
func replaceRequestCookie(req *http.Request, name, value string) {
	kept := req.Cookies()
	req.Header.Del("Cookie")
	for _, c := range kept {
		if c.Name != name {
			req.AddCookie(c)
		}
	}
	if value != "" {
		req.AddCookie(&http.Cookie{Name: name, Value: value}) //nolint:gosec // G124: a request cookie carries only the name and value
	}
}

// copyCSRF gives a redis-store CSRF cookie set in the response a cookie-store copy,
// for a callback that lands at a cookie-mode replica.
func (m *cookieStoreMigration) copyCSRF(w http.ResponseWriter) {
	c, ok := responseCookie(w, redisStoreCSRFName)
	if !ok || c.Value == "" || c.MaxAge < 0 {
		return
	}
	signed, ok := m.resignCSRF(c, cookieStoreCSRFName)
	if !ok {
		return
	}
	copied := *c //nolint:gosec // G124: a copy of the CSRF cookie oauth2-proxy set, with its attributes
	copied.Name = cookieStoreCSRFName
	copied.Value = signed
	http.SetCookie(w, &copied)
}

func (m *cookieStoreMigration) resignCSRF(c *http.Cookie, name string) (string, bool) {
	secret, err := m.proxyConfig.Cookie.GetSecret()
	if err != nil {
		return "", false
	}
	value, signedAt, ok := encryption.Validate(c, secret, m.proxyConfig.Cookie.CSRFExpire)
	if !ok {
		return "", false
	}
	signed, err := encryption.SignedValue(secret, name, value, signedAt)
	if err != nil {
		return "", false
	}
	return signed, true
}
