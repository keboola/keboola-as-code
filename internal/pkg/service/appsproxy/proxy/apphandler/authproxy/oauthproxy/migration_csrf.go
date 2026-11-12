package oauthproxy

import (
	"net/http"

	"github.com/felixge/httpsnoop"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
)

const (
	cookieStoreCSRFName = CookieName + "_csrf"
	redisStoreCSRFName  = redisSessionCookieName + "_csrf"
)

// renameCSRF re-signs a valid cookie-store CSRF cookie under the redis-store name, so a login started at a
// cookie-mode replica finishes at a redis-mode one. The CSRF payload is encrypted with the secret only.
func (m *cookieStoreMigration) renameCSRF(req *http.Request) {
	if req.URL.Path != m.callbackPath || hasCookie(req, redisStoreCSRFName) {
		return
	}
	old, err := req.Cookie(cookieStoreCSRFName)
	if err != nil {
		return
	}
	if signed, ok := m.resignCSRF(old, redisStoreCSRFName); ok {
		req.AddCookie(&http.Cookie{Name: redisStoreCSRFName, Value: signed})
	}
}

// copyCSRF wraps w, so a redis-store CSRF cookie set in the response also gets a cookie-store copy,
// for a callback that lands at a cookie-mode replica.
func (m *cookieStoreMigration) copyCSRF(w http.ResponseWriter) http.ResponseWriter {
	done := false
	addCopy := func() {
		if done {
			return
		}
		done = true
		c, ok := responseCookie(w, redisStoreCSRFName)
		if !ok || c.Value == "" || c.MaxAge < 0 {
			return
		}
		signed, ok := m.resignCSRF(c, cookieStoreCSRFName)
		if !ok {
			return
		}
		copied := *c
		copied.Name = cookieStoreCSRFName
		copied.Value = signed
		http.SetCookie(w, &copied)
	}
	return httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(code int) {
				addCopy()
				next(code)
			}
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(b []byte) (int, error) {
				addCopy()
				return next(b)
			}
		},
	})
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
