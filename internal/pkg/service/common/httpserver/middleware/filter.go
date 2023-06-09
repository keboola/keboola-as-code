package middleware

import (
	"context"
	"net/http"
)

const requestIgnoredCtxKey = ctxKey("request-ignored")

// FilterFn is a predicate used to determine whether a given http.request should
// be logged/traced. A Filter must return true if the request should be logged/traced.
type FilterFn func(*http.Request) bool

func Filter(cfg Config) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			for _, f := range cfg.filters {
				if !f(req) {
					req = req.WithContext(context.WithValue(req.Context(), requestIgnoredCtxKey, true))
					break
				}
			}

			next.ServeHTTP(w, req)
		})
	}
}

// returns true if the request should NOT be logged/traced.
func isRequestIgnored(req *http.Request) bool {
	return req.Context().Value(requestIgnoredCtxKey) == true
}
