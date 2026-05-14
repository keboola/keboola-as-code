package kaipreview

import (
	"net/http"
	"time"
)

const SessionCookieName = "kbc-kai-preview-session"

// SetSessionCookie writes the kai-preview session cookie on w. The cookie is
// host-only (no Domain), Secure, HttpOnly, SameSite=None, Partitioned (CHIPS).
// See docs/superpowers/specs/2026-05-14-dev-iframe-auth-design.md for the full
// attribute rationale.
func SetSessionCookie(w http.ResponseWriter, jwt string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name:        SessionCookieName,
		Value:       jwt,
		Path:        "/",
		Secure:      true,
		HttpOnly:    true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
		MaxAge:      int(ttl.Seconds()),
	})
}

// ClearSessionCookie writes a cookie that invalidates any existing kai-preview
// session cookie on the same host. Used by the exchange endpoint on validation
// failure and by future sign-out flows.
func ClearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:        SessionCookieName,
		Value:       "",
		Path:        "/",
		Secure:      true,
		HttpOnly:    true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
		MaxAge:      -1,
	})
}

// ReadSessionCookie returns the value of the kai-preview session cookie if
// present on the request, or "" if absent or unreadable.
func ReadSessionCookie(r *http.Request) string {
	c, err := r.Cookie(SessionCookieName)
	if err != nil || c == nil {
		return ""
	}
	return c.Value
}
