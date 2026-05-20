package kaipreview

import (
	"net/http"
	"slices"
)

type CORS struct {
	allowedOrigins []string
}

func NewCORS(allowedOrigins []string) *CORS {
	return &CORS{allowedOrigins: allowedOrigins}
}

func (c *CORS) IsAllowed(origin string) bool {
	return slices.Contains(c.allowedOrigins, origin)
}

// HandlePreflight returns true when the request was a preflight OPTIONS and was handled.
// Caller should return immediately if true. Returns false for non-OPTIONS requests.
// withCredentials controls whether Access-Control-Allow-Credentials: true is emitted.
// Use false for endpoints authenticated by request header (e.g. mint/handshake-token which
// uses X-StorageApi-Token), and true for cookie-authenticated endpoints (e.g. refresh).
func (c *CORS) HandlePreflight(w http.ResponseWriter, r *http.Request, withCredentials bool) bool {
	if r.Method != http.MethodOptions {
		return false
	}
	// Emit Vary: Origin on every response (allowed or 403) so shared caches don't
	// key only on URL and serve a 403 cached for one origin to a different origin.
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	if !c.IsAllowed(origin) {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return true
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if withCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-StorageApi-Token, Content-Type")
	w.Header().Set("Access-Control-Max-Age", "600")
	w.WriteHeader(http.StatusNoContent)
	return true
}

// WriteResponseHeaders sets the CORS response headers on a regular (non-preflight) response.
// Call this from the actual handler before writing the body.
// withCredentials controls whether Access-Control-Allow-Credentials: true is emitted.
// Use false for endpoints authenticated by request header (e.g. mint/handshake-token), and
// true for cookie-authenticated endpoints (e.g. refresh).
func (c *CORS) WriteResponseHeaders(w http.ResponseWriter, origin string, withCredentials bool) {
	// Vary: Origin is emitted unconditionally — the response can depend on the Origin
	// header regardless of whether this particular origin is allowed, so caches must
	// key by it. Add (not Set) so we don't clobber any Vary upstream middleware appended.
	w.Header().Add("Vary", "Origin")
	if !c.IsAllowed(origin) {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if withCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}
