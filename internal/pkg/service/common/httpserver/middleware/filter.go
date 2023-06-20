package middleware

import (
	"context"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	disabledTelemetryCtxKey = ctxKey("disabled-telemetry")
	disabledAccessLog       = ctxKey("disabled-access-log")
)

// FilterFn is a predicate used to determine whether a given http.request should
// be logged/traced. A Filter must return true if the request should be logged/traced.
type FilterFn func(*http.Request) bool

func Filter(cfg Config) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(ctxForFilteredRequest(cfg, req)))
		})
	}
}

func ctxForFilteredRequest(cfg Config, req *http.Request) context.Context {
	ctx := req.Context()

	// Filter for whole telemetry
	for _, f := range cfg.filters {
		if !f(req) {
			ctx = context.WithValue(ctx, disabledTelemetryCtxKey, true)
			ctx = context.WithValue(ctx, disabledAccessLog, true)
			ctx = telemetry.ContextWithDisabledTracing(ctx)
			return ctx
		}
	}

	// Filter for access log
	for _, f := range cfg.accessLogFilters {
		if !f(req) {
			return context.WithValue(ctx, disabledAccessLog, true)
		}
	}

	// Filter for tracing
	for _, f := range cfg.tracingFilters {
		if !f(req) {
			return telemetry.ContextWithDisabledTracing(ctx)
		}
	}

	return ctx
}

// skipTelemetryCtxKey returns true if the request should be excluded from telemetry.
func isTelemetryDisabled(req *http.Request) bool {
	return req.Context().Value(disabledTelemetryCtxKey) == true
}

// isAccessLogDisabled returns true if the request should not be logged.
func isAccessLogDisabled(req *http.Request) bool {
	return req.Context().Value(disabledAccessLog) == true
}
