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
			// Capture response
			started := time.Now()
			rw := goaHttpMiddleware.CaptureResponse(w)
			next.ServeHTTP(rw, req)

			// Log
			if !isAccessLogDisabled(req) || rw.StatusCode >= http.StatusInternalServerError {
				requestID, _ := req.Context().Value(RequestIDCtxKey).(string)
				userAgent := req.Header.Get("User-Agent")
				logger := baseLogger.AddPrefix(fmt.Sprintf("[http][requestId=%s]", requestID))
				logger.Infof(
					"req %s status=%d bytes=%d time=%s client_ip=%s agent=%s",
					log.Sanitize(req.URL.String()), rw.StatusCode, rw.ContentLength, time.Since(started).String(),
					log.Sanitize(ip.From(req).String()), userAgent,
				)
			}
		})
	}
}
