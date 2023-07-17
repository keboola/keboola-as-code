package middleware

import (
	"context"
	"net/http"
	"time"
)

func ContextTimout(timeout time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx, cancel := context.WithTimeout(req.Context(), timeout)
			next.ServeHTTP(w, req.WithContext(ctx))
			cancel()
		})
	}
}
