package apphandler

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/basicauth"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/requestcookie"
)

func stripProxyCookies(next chain.Handler) chain.Handler {
	return chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
		req = req.Clone(req.Context())
		requestcookie.Remove(req, isProxyCookie)
		return next.ServeHTTPOrError(w, req)
	})
}

func isProxyCookie(name string) bool {
	switch name {
	case oauthproxy.CookieName,
		selector.ProviderCookie,
		basicauth.CookieName,
		sessions.CookieName,
		session.CookieName:
		return true
	}
	return strings.HasPrefix(name, oauthproxy.CookieName+"_")
}
