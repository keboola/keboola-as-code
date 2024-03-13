package http

import (
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

const attrAppID = "proxy.appid"

func appIDMiddleware(publicURL *url.URL) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			appID, ok := parseAppID(publicURL, req.Host)

			if ok {
				ctx := req.Context()
				ctx = ctxattr.ContextWith(ctx, attribute.String(attrAppID, appID))
				req = req.WithContext(ctx)
			}

			next.ServeHTTP(w, req)
		})
	}
}

func appIDOtelMiddleware() middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			appIDString, ok := ctxattr.Attributes(ctx).Value(attrAppID)
			if ok {
				labeler, _ := otelhttp.LabelerFromContext(ctx)
				labeler.Add(attribute.String(attrAppID, appIDString.Emit()))
			}

			next.ServeHTTP(w, req)
		})
	}
}

func parseAppID(publicURL *url.URL, host string) (string, bool) {
	if !strings.HasSuffix(host, "."+publicURL.Host) {
		return "", false
	}

	if strings.Count(host, ".") != strings.Count(publicURL.Host, ".")+1 {
		return "", false
	}

	idx := strings.IndexByte(host, '.')
	if idx < 0 {
		return "", false
	}

	subdomain := host[:idx]
	idx = strings.LastIndexByte(subdomain, '-')
	if idx < 0 {
		return subdomain, true
	}

	return subdomain[idx+1:], true
}
