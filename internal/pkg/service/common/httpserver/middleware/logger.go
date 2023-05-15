package middleware

import (
	"fmt"
	"net/http"
	"time"

	goaHttpMiddleware "goa.design/goa/v3/http/middleware"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ip"
)

func Logger(baseLogger log.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			started := time.Now()

			// Get request ID
			requestID, _ := req.Context().Value(RequestIDCtxKey).(string)

			// Log request
			userAgent := req.Header.Get("User-Agent")
			logger := baseLogger.AddPrefix(fmt.Sprintf("[http][requestId=%s]", requestID))
			logger.Infof(
				"request %s %s %s %s",
				req.Method, log.Sanitize(req.URL.String()), log.Sanitize(ip.From(req).String()), userAgent,
			)

			// Capture response
			rw := goaHttpMiddleware.CaptureResponse(w)
			next.ServeHTTP(rw, req)

			// Log response
			logger.Infof(
				"response status=%d bytes=%d time=%s agent=%s",
				rw.StatusCode, rw.ContentLength, time.Since(started).String(), userAgent,
			)
		})
	}
}
