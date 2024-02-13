package http

import (
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

type DataApp struct {
	ID           AppID              `json:"id" validator:"required"`
	Name         string             `json:"name" validator:"required"`
	UpstreamHost string             `json:"upstreamUrl" validator:"required"`
	Providers    []options.Provider `json:"providers"`
}

type AppID string

func (v AppID) String() string {
	return string(v)
}

const attrAppID = "proxy.appid"

func appIDMiddleware() middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			appID, ok := parseAppID(req.Host)

			if ok {
				ctx := req.Context()
				ctx = ctxattr.ContextWith(ctx, attribute.String(attrAppID, string(appID)))
				req = req.WithContext(ctx)
			}

			next.ServeHTTP(w, req)
		})
	}
}

func parseAppID(host string) (AppID, bool) {
	if strings.Count(host, ".") != 3 {
		return "", false
	}

	idx := strings.IndexByte(host, '.')
	if idx < 0 {
		return "", false
	}

	subdomain := host[:idx]
	idx = strings.LastIndexByte(subdomain, '-')
	if idx < 0 {
		return AppID(subdomain), true
	}

	return AppID(subdomain[idx+1:]), true
}
