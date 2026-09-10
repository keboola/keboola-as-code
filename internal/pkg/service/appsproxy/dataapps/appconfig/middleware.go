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

func AppConfigFromContext(ctx context.Context) AppConfigResult {
	if appConfig, ok := ctx.Value(appConfigCtxKey).(AppConfigResult); ok {
		return appConfig
	}
	return AppConfigResult{}
}

func Middleware(configLoader Loader, host string) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			workload, ok := resolveWorkload(req, host)
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
func resolveWorkload(req *http.Request, host string) (k8sapp.WorkloadRef, bool) {
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
