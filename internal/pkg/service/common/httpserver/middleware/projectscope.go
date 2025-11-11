package middleware

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

// ProjectScopeConfig defines configuration for ProjectScope middleware.
// This middleware attempts to create a project-scoped request early in the request lifecycle
// so that downstream middlewares (like Logger) can access project-specific attributes.
type ProjectScopeConfig struct {
	// ProjectScopeCtxKey is the context key where ProjectRequestScope will be stored
	ProjectScopeCtxKey any
	// PublicScopeCtxKey is the context key where PublicRequestScope is stored
	PublicScopeCtxKey any
	// TokenHeader is the HTTP header name containing the authentication token (e.g., "X-StorageApi-Token")
	TokenHeader string
	// CreateProjectScope is a factory function that creates a project scope from public scope and token
	// It should return (projectScope, nil) on success or (nil, error) on failure
	CreateProjectScope func(ctx context.Context, publicScope any, token string) (any, error)
	// AttributeExtractor is an optional function that extracts telemetry attributes from the created project scope.
	// If provided, the middleware will enrich the context and active span with these attributes immediately.
	// This allows downstream middlewares (especially Logger) to access project attributes without needing
	// a separate Telemetry middleware for project-level attributes.
	AttributeExtractor func(projectScope any) []attribute.KeyValue
}

// ProjectScope middleware attempts to create a ProjectRequestScope early in the request lifecycle.
// This allows downstream middlewares (especially Logger) to access project-specific attributes.
//
// The middleware:
// 1. Checks if ProjectRequestScope already exists in context (from auth middleware)
// 2. If not, retrieves PublicRequestScope from context
// 3. Attempts to create ProjectRequestScope from the authentication token header
// 4. Stores ProjectRequestScope in context for downstream middlewares
// 5. If AttributeExtractor is provided, enriches context and span with project attributes
//
// This middleware should be registered in BeforeLoggerMiddlewares so that the Logger middleware
// can access enriched project attributes.
func ProjectScope(cfg ProjectScopeConfig) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Check if project scope already exists
			var projectScope any
			if v := ctx.Value(cfg.ProjectScopeCtxKey); v != nil {
				projectScope = v
			}

			// If no project scope yet, try to create it early from token
			if projectScope == nil {
				// Get public scope
				publicScope := ctx.Value(cfg.PublicScopeCtxKey)
				if publicScope != nil {
					// Try to get token from header
					token := req.Header.Get(cfg.TokenHeader)
					if token != "" {
						// Attempt to create project scope
						if ps, err := cfg.CreateProjectScope(ctx, publicScope, token); err == nil {
							projectScope = ps
							// Store project scope in context
							ctx = context.WithValue(ctx, cfg.ProjectScopeCtxKey, projectScope)
							req = req.WithContext(ctx)
						}
						// Note: We silently ignore errors here because:
						// - This is an optimization for early scope creation
						// - The actual auth middleware will handle auth errors properly
						// - We don't want to block requests if early scope creation fails
					}
				}
			}

			// If AttributeExtractor is provided and we have a project scope, enrich context with attributes
			if projectScope != nil && cfg.AttributeExtractor != nil {
				attrs := cfg.AttributeExtractor(projectScope)
				if len(attrs) > 0 {
					// Enrich context with attributes (for Logger middleware)
					ctx = ctxattr.ContextWith(ctx, attrs...)

					// Enrich active span with attributes (for OpenTelemetry tracing)
					if span, found := RequestSpan(ctx); found {
						span.SetAttributes(attrs...)
					}

					// Update request with enriched context
					req = req.WithContext(ctx)

					// CRITICAL: Store the updated request in RequestCtxKey so that downstream
					// middlewares (like Logger) can retrieve the request with all enriched attributes
					ctx = context.WithValue(ctx, RequestCtxKey, req)
					req = req.WithContext(ctx)
				}
			}

			next.ServeHTTP(w, req)
		})
	}
}
