package middleware

import (
	"net/http"
	"strings"
)

// BearerToken promotes an `Authorization: Bearer <token>` value into the given
// storage-token header when that header is not already set. This lets clients
// authenticate with a programmatic token (kbc_at_*/kbc_pat_*) via the standard
// Authorization header while the rest of the stack (Goa security decoder,
// ProjectScope middleware, token exchange) keeps reading a single header.
//
// It must run before any middleware or decoder that reads tokenHeader.
func BearerToken(tokenHeader string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Header.Get(tokenHeader) == "" {
				if token := bearerToken(req.Header.Get("Authorization")); token != "" {
					req.Header.Set(tokenHeader, token)
				}
			}
			next.ServeHTTP(w, req)
		})
	}
}

// bearerToken extracts the token from a "Bearer <token>" Authorization value,
// or "" if the scheme is absent or not Bearer.
func bearerToken(authorization string) string {
	const prefix = "Bearer "
	if len(authorization) > len(prefix) && strings.EqualFold(authorization[:len(prefix)], prefix) {
		return strings.TrimSpace(authorization[len(prefix):])
	}
	return ""
}
