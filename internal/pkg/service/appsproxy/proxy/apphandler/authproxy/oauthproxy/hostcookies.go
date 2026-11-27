package oauthproxy

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/cookiemigration"
)

const csrfSuffix = "_csrf"

// withLegacyCookies serves a request that carries the session ticket and the CSRF cookies under the host-only names
// as if it carried them under the names oauth2-proxy is configured with. Both are signed together with their name,
// so each is validated with the secret of this provider under the host-only name and signed again for the configured one.
// When both names are present, the cookie signed later is used.
func (h *Handler) withLegacyCookies(req *http.Request) *http.Request {
	secret, err := h.proxyConfig.Cookie.GetSecret()
	if err != nil {
		return req
	}

	req, _ = cookiemigration.Resign(req, HostSessionCookieName, SessionCookieName, secret, h.proxyConfig.Cookie.Expire)

	for _, c := range req.Cookies() {
		suffix, ok := strings.CutPrefix(c.Name, HostSessionCookieName)
		if !ok || !strings.HasSuffix(suffix, csrfSuffix) {
			continue
		}
		req, _ = cookiemigration.Resign(req, c.Name, SessionCookieName+suffix, secret, h.proxyConfig.Cookie.CSRFExpire)
	}
	return req
}
