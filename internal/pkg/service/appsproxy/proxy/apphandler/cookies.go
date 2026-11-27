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
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/cookiemigration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/requestcookie"
)

func stripProxyCookies(next chain.Handler) chain.Handler {
	return chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
		req = req.Clone(req.Context())
		requestcookie.Remove(req, isProxyCookie)
		return next.ServeHTTPOrError(w, req)
	})
}

// copyHostCookies serves a request that carries the host-only name of a cookie under the legacy name the auth handlers read.
// The session ticket is not handled here: its signature depends on the provider, see oauthproxy.Handler.
func copyHostCookies(req *http.Request) *http.Request {
	req = cookiemigration.Copy(req, sessions.HostCookieName, sessions.CookieName)
	req = cookiemigration.Copy(req, basicauth.HostCookieName, basicauth.CookieName)
	return cookiemigration.Copy(req, selector.HostProviderCookie, selector.ProviderCookie)
}

// expireHostCookies expires every host-only cookie, so a sign-out leaves no session behind
// even when the browser got the cookies from a pod that writes the host-only names and does not send them here.
func expireHostCookies(w http.ResponseWriter) {
	for _, name := range []string{
		sessions.HostCookieName,
		basicauth.HostCookieName,
		selector.HostProviderCookie,
		oauthproxy.HostSessionCookieName,
		oauthproxy.HostSessionCookieName + "_csrf",
	} {
		http.SetCookie(w, hostCookieExpiry(name))
	}
}

func hostCookieExpiry(name string) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Path:     "/",
		MaxAge:   -1,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}
}

func isHostCookie(name string) bool {
	switch name {
	case sessions.HostCookieName, basicauth.HostCookieName, selector.HostProviderCookie, oauthproxy.HostSessionCookieName:
		return true
	}
	return strings.HasPrefix(name, oauthproxy.HostSessionCookieName+"_")
}

func isProxyCookie(name string) bool {
	switch name {
	case oauthproxy.CookieName,
		selector.ProviderCookie,
		basicauth.CookieName,
		sessions.CookieName,
		session.CookieName,
		selector.HostProviderCookie,
		basicauth.HostCookieName,
		sessions.HostCookieName,
		oauthproxy.HostSessionCookieName:
		return true
	}
	return strings.HasPrefix(name, oauthproxy.CookieName+"_") || strings.HasPrefix(name, oauthproxy.HostSessionCookieName+"_")
}
