package appconfig

import (
	"context"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

type ctxKey string

const (
	AppIDCtxKey       = ctxKey("app-id")
	AppConfigCtxKey   = ctxKey("app-config")
	AppModifiedCtxKey = ctxKey("app-modified")
	AppErrorCtxKey    = ctxKey("app-error")
)

func Middleware(configLoader *Loader, host string) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			appID, ok := parseAppID(req, host)
			if ok {
				ctx := req.Context()
				ctx = context.WithValue(ctx, AppIDCtxKey, appID)

				appConfig, modified, err := configLoader.GetConfig(ctx, appID)

				if err != nil {
					ctx = context.WithValue(ctx, AppErrorCtxKey, err)
				} else {
					ctx = context.WithValue(ctx, AppConfigCtxKey, appConfig)
					ctx = context.WithValue(ctx, AppModifiedCtxKey, modified)
					ctx = ctxattr.ContextWith(ctx, appConfig.Telemetry()...)
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
		return api.AppID(subdomain[lastDash+1:]), true
	}

	return api.AppID(subdomain), true
}
