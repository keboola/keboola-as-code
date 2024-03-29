package middleware

import (
	"net/http"
	"time"

	goaHttpMiddleware "goa.design/goa/v3/http/middleware"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ip"
)

func Logger(baseLogger log.Logger) Middleware {
	logger := baseLogger.WithComponent("http")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Skip access log if it is disabled
			if isAccessLogDisabled(req) {
				next.ServeHTTP(w, req)
				return
			}

			// Capture response
			started := time.Now()
			rw := goaHttpMiddleware.CaptureResponse(w) //nolint:staticcheck // deprecated, information should be loaded from OTEL span
			next.ServeHTTP(rw, req)

			// Log
			userAgent := req.Header.Get("User-Agent")
			logger.Infof(
				req.Context(),
				"req %s status=%d bytes=%d time=%s client_ip=%s agent=%s",
				log.Sanitize(req.URL.String()), rw.StatusCode, rw.ContentLength, time.Since(started).String(),
				log.Sanitize(ip.From(req).String()), userAgent,
			)
		})
	}
}
