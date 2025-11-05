package middleware

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	httpmw "github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	templatesDeps "github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
)

const storageAPITokenHeader = "X-StorageAPI-Token"

// Telemetry returns an application middleware for the Templates service that enriches
// request context and root span with internal attributes useful for observability/Datadog.
//
// The middleware attaches attributes when available, for example:
//   - template.projectId
//   - template.http.request_id
//   - template.branch.id (if available from route params)
//
// The attributes are added both to the context (for downstream operations) and
// to the current request span, if present.
//
// The middleware stores the updated request in RequestCtxKey so that the Logger middleware
// can retrieve it and include the enriched attributes in the log output.
//
// Note: Route params (e.g., branch.id) are only available after routing, so they may not
// be present when this middleware runs as a server-level middleware.
func Telemetry() httpmw.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Prefer project scope if present, otherwise use public scope.
			var (
				projectScope templatesDeps.ProjectRequestScope
				publicScope  templatesDeps.PublicRequestScope
				ok           bool
			)

			if v := ctx.Value(templatesDeps.ProjectRequestScopeCtxKey); v != nil {
				if s, valid := v.(templatesDeps.ProjectRequestScope); valid {
					projectScope = s
					ok = true
				}
			}
			if !ok {
				if v := ctx.Value(templatesDeps.PublicRequestScopeCtxKey); v != nil {
					if s, valid := v.(templatesDeps.PublicRequestScope); valid {
						publicScope = s
					}
				}
			}

			// If no project scope yet, try to derive it early from token so access logger can include attrs.
			if projectScope == nil {
				token := req.Header.Get(storageAPITokenHeader)
				if token != "" && publicScope != nil {
					if prjScope, err := templatesDeps.NewProjectRequestScope(ctx, publicScope, token); err == nil {
						projectScope = prjScope
						// Store project scope in context so downstream code can use it.
						ctx = context.WithValue(ctx, templatesDeps.ProjectRequestScopeCtxKey, prjScope)
					}
				}
			}

			// Collect attributes.
			var attrs []attribute.KeyValue

			// Request ID (available via RequestInfo in both scopes).
			switch {
			case projectScope != nil:
				// Project specific attributes.
				attrs = append(attrs, attribute.String("template.projectId", projectScope.ProjectID().String()))
			}

			// Try to enrich with branch id from router params if available and numeric.

			const prefix = "/v1/project/"
			path := req.URL.Path
			if strings.HasPrefix(path, prefix) {
				rest := path[len(prefix):]
				// take first segment
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					rest = rest[:i]
				}
				if rest != "" {
					if _, err := strconv.Atoi(rest); err == nil {
						attrs = append(attrs, attribute.String("template.branch.id", rest))
					}
				}
			}

			if len(attrs) > 0 {
				// Enrich context for downstream code.
				ctx = ctxattr.ContextWith(ctx, attrs...)

				// Enrich root span if present.
				if span, found := httpmw.RequestSpan(ctx); found {
					span.SetAttributes(attrs...)
				}

				// Update request with enriched context.
				req = req.WithContext(ctx)

				// Make the updated request discoverable by outer middlewares (e.g., access logger).
				// Store the FINAL updated request (after all context updates) in RequestCtxKey.
				// This allows Logger middleware to retrieve the request with all attributes.
				ctx = context.WithValue(ctx, httpmw.RequestCtxKey, req)
				req = req.WithContext(ctx)
			}

			next.ServeHTTP(w, req)
		})
	}
}
