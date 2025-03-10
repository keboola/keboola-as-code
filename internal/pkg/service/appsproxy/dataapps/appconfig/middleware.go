package appconfig

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

type ctxKey string

const (
	appConfigCtxKey = ctxKey("app-config")
)

type AppConfigResult struct {
	AppID     api.AppID
	AppConfig api.AppConfig
	Modified  bool
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
			appID, ok := parseAppID(req, host)
			if ok {
				ctx := req.Context()

				appConfig, modified, err := configLoader.GetConfig(ctx, appID)
				result := AppConfigResult{
					AppID:     appID,
					AppConfig: appConfig,
					Modified:  modified,
					Err:       err,
				}

				ctx = context.WithValue(ctx, appConfigCtxKey, result)
				if err == nil {
					ctx = ctxattr.ContextWith(ctx, appConfig.Telemetry()...)
					// Duplicate app ID in the event attributes under the same key as sandboxes service.
					ctx = ctxattr.ContextWith(ctx, attribute.String("context.appId", string(appID)))
				}

				req = req.WithContext(ctx)
			}

			next.ServeHTTP(w, req)
		})
	}
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
