package middleware

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

// AttributeExtractor is a function that extracts telemetry attributes from the request context.
// It's called by the Telemetry middleware to collect service-specific attributes.
type AttributeExtractor func(ctx context.Context, req *http.Request) []attribute.KeyValue

// RouteParamExtractor is a function that extracts a route parameter value from the request context.
// It returns the parameter value and true if found, or empty string and false if not found.
type RouteParamExtractor func(ctx context.Context, paramName string) (string, bool)

// RouteAttributes creates an AttributeExtractor that extracts route parameters and converts them
// to telemetry attributes. This is useful for adding route-specific context like branch IDs, project IDs, etc.
//
// Parameters:
//   - extractor: A function that knows how to extract route parameters from the request context
//   - mappings: A map of parameter names to attribute keys (e.g., {"branchId": "service.branch.id"})
//
// Example usage:
//
//	middleware.Telemetry(
//	    middleware.RouteAttributes(
//	        middleware.TreeMuxParamExtractor,
//	        map[string]string{"branchId": "stream.branch.id"},
//	    ),
//	)
func RouteAttributes(extractor RouteParamExtractor, mappings map[string]string) AttributeExtractor {
	return func(ctx context.Context, req *http.Request) []attribute.KeyValue {
		var attrs []attribute.KeyValue
		for paramName, attrKey := range mappings {
			if value, found := extractor(ctx, paramName); found && value != "" {
				attrs = append(attrs, attribute.String(attrKey, value))
			}
		}
		return attrs
	}
}

// TreeMuxParamExtractor extracts route parameters from httptreemux context.
// This is used by services that use httptreemux router (e.g., stream API).
// It validates numeric parameters (like branch IDs) before returning them.
func TreeMuxParamExtractor(ctx context.Context, paramName string) (string, bool) {
	params := httptreemux.ContextParams(ctx)
	if params == nil {
		return "", false
	}

	value, found := params[paramName]
	if !found || value == "" {
		return "", false
	}

	// Validate numeric parameters (like branchId)
	if strings.Contains(strings.ToLower(paramName), "id") {
		if _, err := strconv.Atoi(value); err != nil {
			return "", false
		}
	}

	return value, true
}

// URLPathExtractor creates a RouteParamExtractor that extracts parameters from URL path segments.
// This is useful for services that need to parse the URL path before routing is complete.
//
// Parameters:
//   - pathPrefix: The URL path prefix to match (e.g., "/v1/project/")
//   - segmentIndex: The 0-based index of the segment after the prefix (e.g., 0 for branch ID in "/v1/project/{branchId}/...")
//
// Example:
//
//	// For URL "/v1/project/123/branch", extract "123" as branchId:
//	extractor := URLPathExtractor("/v1/project/", 0)
//	value, found := extractor(ctx, "branchId") // returns "123", true
func URLPathExtractor(pathPrefix string, segmentIndex int) RouteParamExtractor {
	return func(ctx context.Context, paramName string) (string, bool) {
		// Get request from context
		req, ok := ctx.Value(RequestCtxKey).(*http.Request)
		if !ok {
			return "", false
		}

		path := req.URL.Path
		if !strings.HasPrefix(path, pathPrefix) {
			return "", false
		}

		rest := path[len(pathPrefix):]
		segments := strings.Split(rest, "/")

		if segmentIndex >= len(segments) || segments[segmentIndex] == "" {
			return "", false
		}

		value := segments[segmentIndex]

		// Validate numeric parameters (like branchId)
		if strings.Contains(strings.ToLower(paramName), "id") {
			if _, err := strconv.Atoi(value); err != nil {
				return "", false
			}
		}

		return value, true
	}
}

// Telemetry middleware enriches the request context and OpenTelemetry span with service-specific attributes.
//
// The middleware:
// 1. Calls the AttributeExtractor to collect service-specific attributes from the request
// 2. Enriches the context with these attributes (for Logger middleware)
// 3. Enriches the active OpenTelemetry span with these attributes (for tracing)
// 4. Updates the request in RequestCtxKey so downstream middlewares can access the enriched request
//
// This middleware should be registered in BeforeLoggerMiddlewares so that the Logger middleware
// can access the enriched attributes when logging requests.
//
// Example usage:
//
//	middleware.Telemetry(func(ctx context.Context, req *http.Request) []attribute.KeyValue {
//	    var attrs []attribute.KeyValue
//	    if projectScope := getProjectScope(ctx); projectScope != nil {
//	        attrs = append(attrs, attribute.String("service.projectId", projectScope.ProjectID().String()))
//	    }
//	    return attrs
//	})
func Telemetry(extractor AttributeExtractor) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Extract service-specific attributes
			attrs := extractor(ctx, req)

			// Enrich context and span if attributes were collected
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

			next.ServeHTTP(w, req)
		})
	}
}
