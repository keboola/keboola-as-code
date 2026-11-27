package oauthproxy

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/cookiemigration"
)

const csrfSuffix = "_csrf"

// withLegacyCookies serves a request that carries the session ticket and the CSRF cookies under the host-only names
// as if it carried them under the names oauth2-proxy is configured with. Both are signed together with their name,
// so each is validated under the host-only name and signed again for the configured one.
//
// The returned writer expires a host-only CSRF cookie whenever oauth2-proxy sets its legacy counterpart:
// a used one must not outlive the login, because a pod that prefers the host-only name would read the stale cookie.
func (h *Handler) withLegacyCookies(w http.ResponseWriter, req *http.Request) (http.ResponseWriter, *http.Request) {
	secret, err := h.proxyConfig.Cookie.GetSecret()
	if err != nil {
		return w, req
	}

	req, _ = cookiemigration.Resign(req, HostSessionCookieName, sessionCookieName, secret, h.proxyConfig.Cookie.Expire)

	var carried []string
	for _, c := range req.Cookies() {
		suffix, ok := strings.CutPrefix(c.Name, HostSessionCookieName)
		if !ok || !strings.HasSuffix(suffix, csrfSuffix) {
			continue
		}
		var translated bool
		req, translated = cookiemigration.Resign(req, c.Name, sessionCookieName+suffix, secret, h.proxyConfig.Cookie.CSRFExpire)
		if translated {
			carried = append(carried, suffix)
		}
	}
	if len(carried) == 0 {
		return w, req
	}
	return &csrfExpiryWriter{ResponseWriter: w, suffixes: carried}, req
}

type csrfExpiryWriter struct {
	http.ResponseWriter
	suffixes    []string
	wroteHeader bool
}

func (w *csrfExpiryWriter) WriteHeader(status int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.expireHostCSRF()
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *csrfExpiryWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *csrfExpiryWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *csrfExpiryWriter) expireHostCSRF() {
	setNames := make(map[string]bool)
	for _, line := range w.Header().Values("Set-Cookie") {
		name, _, _ := strings.Cut(line, "=")
		setNames[name] = true
	}
	for _, suffix := range w.suffixes {
		if !setNames[sessionCookieName+suffix] {
			continue
		}
		http.SetCookie(w.ResponseWriter, &http.Cookie{
			Name:     HostSessionCookieName + suffix,
			Path:     "/",
			MaxAge:   -1,
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})
	}
}
