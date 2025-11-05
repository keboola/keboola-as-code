package middleware

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
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
			// Use updated request from context if available, to include ctxattrs injected by inner middlewares.
			// Service-specific middlewares (e.g., Telemetry) store the enriched request
			// in RequestCtxKey, allowing Logger to include those attributes in the log output.
			ctx := req.Context()
			if updatedReq, ok := RequestValue(ctx); ok && updatedReq != nil {
				ctx = updatedReq.Context()
			}

			userAgent := req.Header.Get("User-Agent")
			logger.
				With(
					attribute.Int("http.status", rw.StatusCode),
					attribute.Int("http.bytes", rw.ContentLength),
					attribute.String("http.time", time.Since(started).String()),
					attribute.String("http.client.ip", log.Sanitize(ip.From(req).String())),
					attribute.String("http.client.agent", userAgent),
				).
				Infof(ctx, "req %d %s", rw.StatusCode, log.Sanitize(req.URL.String()))
		})
	}
}
