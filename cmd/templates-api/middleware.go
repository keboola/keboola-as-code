package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	httpMiddleware "goa.design/goa/v3/http/middleware"
	"goa.design/goa/v3/middleware"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
)

func ContextMiddleware(d dependencies.Container, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Generate unique request ID
		requestId := idgenerator.RequestId()
		ctx := context.WithValue(d.Ctx(), middleware.RequestIDKey, requestId)
		// Include request ID in all log messages
		loggerPrefix := fmt.Sprintf("[requestId=%s]", requestId)
		// Add dependencies to the context
		ctx = context.WithValue(ctx, dependencies.CtxKey, d.WithLoggerPrefix(loggerPrefix))
		// Add request ID to headers
		w.Header().Add("X-Request-Id", requestId)
		// Handle
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

func LogMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		logger := r.Context().Value(dependencies.CtxKey).(dependencies.Container).PrefixLogger()

		// Log request
		logger.
			WithAdditionalPrefix("[request]").
			Infof("%s %s %s", r.Method, r.URL.String(), from(r))

		// Capture response
		rw := httpMiddleware.CaptureResponse(w)
		h.ServeHTTP(rw, r)

		// Log response
		logger.
			WithAdditionalPrefix("[response]").
			Infof("status=%d bytes=%d time=%s", rw.StatusCode, rw.ContentLength, time.Since(started).String())
	})
}

// from computes the request client IP.
func from(req *http.Request) string {
	if f := req.Header.Get("X-Forwarded-For"); f != "" {
		return f
	}
	f := req.RemoteAddr
	ip, _, err := net.SplitHostPort(f)
	if err != nil {
		return f
	}
	return ip
}
