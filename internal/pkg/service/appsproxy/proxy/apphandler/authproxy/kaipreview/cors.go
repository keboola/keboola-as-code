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
func (c *CORS) HandlePreflight(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodOptions {
		return false
	}
	origin := r.Header.Get("Origin")
	if !c.IsAllowed(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return true
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-StorageApi-Token, Content-Type")
	w.Header().Set("Access-Control-Max-Age", "600")
	w.WriteHeader(http.StatusNoContent)
	return true
}

// WriteResponseHeaders sets the CORS response headers on a regular (non-preflight) response.
// Call this from the actual handler before writing the body.
func (c *CORS) WriteResponseHeaders(w http.ResponseWriter, origin string) {
	if !c.IsAllowed(origin) {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Vary", "Origin")
}
