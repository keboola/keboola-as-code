// Package cookiemigration lets a request that carries a cookie under one name be served as if it carried it under another.
package cookiemigration

import (
	"net/http"
	"time"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
)

// Copy returns a request with cookie from also present under the name to.
// A cookie already present under to is kept. The request is cloned only when a cookie is added.
func Copy(req *http.Request, from, to string) *http.Request {
	if _, err := req.Cookie(to); err == nil {
		return req
	}
	c, err := req.Cookie(from)
	if err != nil {
		return req
	}
	req = req.Clone(req.Context())
	req.AddCookie(&http.Cookie{Name: to, Value: c.Value}) //nolint:gosec // G124: a request cookie, only its name and value are sent
	return req
}

// Resign is Copy for cookies that oauth2-proxy signs together with their name.
// The value is validated under from and signed again under to with its original timestamp,
// so the cookie does not live longer than the one the browser holds.
// It reports false, and returns req unchanged, when nothing was added.
func Resign(req *http.Request, from, to, secret string, expire time.Duration) (*http.Request, bool) {
	if _, err := req.Cookie(to); err == nil {
		return req, false
	}
	c, err := req.Cookie(from)
	if err != nil {
		return req, false
	}
	value, issuedAt, ok := encryption.Validate(c, secret, expire)
	if !ok {
		return req, false
	}
	resigned, err := encryption.SignedValue(secret, to, value, issuedAt)
	if err != nil {
		return req, false
	}
	req = req.Clone(req.Context())
	req.AddCookie(&http.Cookie{Name: to, Value: resigned}) //nolint:gosec // G124: a request cookie, only its name and value are sent
	return req, true
}
