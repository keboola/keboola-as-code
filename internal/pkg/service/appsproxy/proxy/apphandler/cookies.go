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
		oauthproxy.CookieName + "_csrf",
		selector.ProviderCookie,
		basicauth.CookieName,
		sessions.CookieName,
		session.CookieName:
		return true
	}
	return isSplitSessionCookie(name)
}

func isSplitSessionCookie(name string) bool {
	index, ok := strings.CutPrefix(name, oauthproxy.CookieName+"_")
	if !ok || index == "" {
		return false
	}
	for _, r := range index {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
