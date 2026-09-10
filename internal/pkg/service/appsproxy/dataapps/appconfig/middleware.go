package appconfig

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

type ctxKey string

const (
	appConfigCtxKey = ctxKey("app-config")
)

type AppConfigResult struct {
	AppID     api.AppID
	Workload  k8sapp.WorkloadRef
	AppConfig api.AppConfig
	Err       error
}

// WorkloadResolver reports the workload that owns an exact hostname, if any.
type WorkloadResolver interface {
	ResolveHost(ctx context.Context, host string) (k8sapp.WorkloadRef, bool)
}

func AppConfigFromContext(ctx context.Context) AppConfigResult {
	if appConfig, ok := ctx.Value(appConfigCtxKey).(AppConfigResult); ok {
		return appConfig
	}
	return AppConfigResult{}
}

func Middleware(configLoader Loader, resolver WorkloadResolver, host string) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			workload, ok := resolveWorkload(req, resolver, host)
			if ok {
				ctx := req.Context()
				appID := workload.AppID

				appConfig, err := configLoader.GetConfig(ctx, appID)
				result := AppConfigResult{
					AppID:     appID,
					Workload:  workload,
					AppConfig: appConfig,
					Err:       err,
				}

				ctx = context.WithValue(ctx, appConfigCtxKey, result)
				if err == nil {
					// Enrich context with telemetry attributes for downstream operations.
					telemetryAttrs := appConfig.Telemetry()
					// Duplicate app ID in the event attributes under the same key as sandboxes service.
					telemetryAttrs = append(telemetryAttrs, attribute.String("context.appId", string(appID)))

					ctx = ctxattr.ContextWith(ctx, telemetryAttrs...)

					// Enrich active request span if present.
					if span, found := middleware.RequestSpan(ctx); found {
						span.SetAttributes(telemetryAttrs...)
					}

					// Update request with enriched context.
					req = req.WithContext(ctx)

					// Make the updated request discoverable by outer middlewares (e.g., access logger).
					// Store the FINAL updated request (after all context updates) in RequestCtxKey.
					// This allows Logger middleware to retrieve the request with all attributes.
					ctx = context.WithValue(ctx, middleware.RequestCtxKey, req)
					req = req.WithContext(ctx)
				} else {
					// Even if there's an error, update the request with the result for error handling.
					req = req.WithContext(ctx)
				}
			}

			next.ServeHTTP(w, req)
		})
	}
}

// resolveWorkload picks the workload for the request hostname.
//
// A Sandbox that owns the exact hostname is authoritative and supplies the
// appId from its own spec, so such a hostname does not have to contain one.
// Everything else falls through to the unchanged App normalisation.
func resolveWorkload(req *http.Request, resolver WorkloadResolver, host string) (k8sapp.WorkloadRef, bool) {
	// parseAppID enforces this for the App path. The exact-hostname path needs
	// it too, or a hostname indexed outside the proxy's own domain would route.
	if !strings.HasSuffix(k8sapp.NormalizeHost(req.Host), "."+k8sapp.NormalizeHost(host)) {
		return k8sapp.WorkloadRef{}, false
	}

	if ref, ok := resolver.ResolveHost(req.Context(), req.Host); ok {
		return ref, true
	}

	appID, ok := parseAppID(req, host)
	if !ok {
		return k8sapp.WorkloadRef{}, false
	}
	return k8sapp.WorkloadRef{AppID: appID}, true
}

func parseAppID(req *http.Request, host string) (api.AppID, bool) {
	// Request domain must match expected public domain
	domain := req.Host // not req.URL.Host, see URL field docs "For most requests, fields other than Path and RawQuery will be empty."
	if !strings.HasSuffix(domain, "."+host) {
		return "", false
	}

	// Only one subdomain is allowed
	if strings.Count(domain, ".") != strings.Count(host, ".")+1 {
		return "", false
	}

	// Get subdomain
	subdomain := domain[:strings.IndexByte(domain, '.')]

	// Remove optional app name prefix, if any
	lastDash := strings.LastIndexByte(subdomain, '-')
	if lastDash >= 0 {
		subdomain = subdomain[lastDash+1:]
	}

	return api.AppID(subdomain), true
}
