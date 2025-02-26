package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func ContextTimeout(timeout time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx, cancel := context.WithTimeoutCause(req.Context(), timeout, errors.New("request timeout"))
			next.ServeHTTP(w, req.WithContext(ctx))
			cancel()
		})
	}
}
