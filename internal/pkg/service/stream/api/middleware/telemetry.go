package middleware

import (
	"context"
	"net/http"
	"strconv"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	httpmw "github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

const storageAPITokenHeader = "X-StorageAPI-Token"

// Telemetry enriches request context and active span with attributes used for observability.
// It aims to run before access Logger so that logs include attributes.
// Injected attributes when available:
//   - stream.http.request_id
//   - stream.stackHost
//   - stream.projectId
//   - stream.branch.id (if available after routing)
func Telemetry() httpmw.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			// Prefer project scope if present, otherwise use public scope.
			var (
				projectScope dependencies.ProjectRequestScope
				publicScope  dependencies.PublicRequestScope
				ok           bool
			)

			if v := ctx.Value(dependencies.ProjectRequestScopeCtxKey); v != nil {
				if s, valid := v.(dependencies.ProjectRequestScope); valid {
					projectScope = s
					ok = true
				}
			}
			if !ok {
				if v := ctx.Value(dependencies.PublicRequestScopeCtxKey); v != nil {
					if s, valid := v.(dependencies.PublicRequestScope); valid {
						publicScope = s
					}
				}
			}

			// If no project scope yet, derive it early from token to enrich access logs.
			if projectScope == nil {
				token := req.Header.Get(storageAPITokenHeader)
				if token != "" && publicScope != nil {
					if prjScope, err := dependencies.NewProjectRequestScope(ctx, publicScope, token); err == nil {
						projectScope = prjScope
						ctx = context.WithValue(ctx, dependencies.ProjectRequestScopeCtxKey, prjScope)
					}
				}
			}

			// If no project scope yet, try to derive it early from token so access logger can include attrs.
			if projectScope == nil {
				token := req.Header.Get(storageAPITokenHeader)
				if token != "" && publicScope != nil {
					if prjScope, err := dependencies.NewProjectRequestScope(ctx, publicScope, token); err == nil {
						projectScope = prjScope
						// Store project scope in context so downstream code can use it.
						ctx = context.WithValue(ctx, dependencies.ProjectRequestScopeCtxKey, prjScope)
					}
				}
			}

			// Collect attributes
			var attrs []attribute.KeyValue
			switch {
			case projectScope != nil:
				attrs = append(attrs, attribute.String("stream.projectId", projectScope.ProjectID().String()))

			}

			// Try to add branch id if available after routing.
			if params := httptreemux.ContextParams(ctx); params != nil {
				if v, found := params["branchId"]; found && v != "" {
					if _, err := strconv.Atoi(v); err == nil {
						attrs = append(attrs, attribute.String("stream.branch.id", v))
					}
				}
			}

			if len(attrs) > 0 {
				ctx = ctxattr.ContextWith(ctx, attrs...)
				if span, found := httpmw.RequestSpan(ctx); found {
					span.SetAttributes(attrs...)
				}
				req = req.WithContext(ctx)
				ctx = context.WithValue(ctx, httpmw.RequestCtxKey, req)
				req = req.WithContext(ctx)
			}

			next.ServeHTTP(w, req)
		})
	}
}
