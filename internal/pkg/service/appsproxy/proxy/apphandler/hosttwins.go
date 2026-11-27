package apphandler

import (
	"bufio"
	"net"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/basicauth"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
)

// hostTwin returns the host-only name that goes with a legacy cookie name.
func hostTwin(legacy string) (string, bool) {
	switch legacy {
	case sessions.CookieName:
		return sessions.HostCookieName, true
	case basicauth.CookieName:
		return basicauth.HostCookieName, true
	case selector.ProviderCookie:
		return selector.HostProviderCookie, true
	}
	if rest, ok := strings.CutPrefix(legacy, oauthproxy.SessionCookieName); ok && (rest == "" || strings.HasPrefix(rest, "_")) {
		return oauthproxy.HostSessionCookieName + rest, true
	}
	return "", false
}

// withTwinExpiry makes the browser hold one set of cookie names.
// Whenever the response sets or clears a legacy cookie and the request carried its host-only twin,
// the twin is expired in the same response. Otherwise a pod that prefers the host-only names
// would read a stale twin next to the cookie that was just written.
// Requests without host-only cookies are served unchanged.
func withTwinExpiry(w http.ResponseWriter, req *http.Request) http.ResponseWriter {
	carried := make(map[string]bool)
	for _, c := range req.Cookies() {
		if isHostCookie(c.Name) {
			carried[c.Name] = true
		}
	}
	if len(carried) == 0 {
		return w
	}
	return &twinExpiryWriter{ResponseWriter: w, carried: carried}
}

type twinExpiryWriter struct {
	http.ResponseWriter
	carried map[string]bool
	done    bool
}

func (w *twinExpiryWriter) WriteHeader(status int) {
	w.expireTwins()
	w.ResponseWriter.WriteHeader(status)
}

func (w *twinExpiryWriter) Write(b []byte) (int, error) {
	w.expireTwins()
	return w.ResponseWriter.Write(b)
}

// Flush and Hijack are looked up on the writer by type assertion, also by oauth2-proxy.
func (w *twinExpiryWriter) Flush() {
	w.expireTwins()
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// Hijack runs the expiry first: a 101 is written from the header map and never goes through WriteHeader.
func (w *twinExpiryWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.expireTwins()
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *twinExpiryWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *twinExpiryWriter) expireTwins() {
	if w.done {
		return
	}
	w.done = true

	header := w.Header()
	set := make(map[string]bool)
	lines := header.Values("Set-Cookie")
	for _, line := range lines {
		name, _, _ := strings.Cut(line, "=")
		set[name] = true
	}
	for _, line := range lines {
		name, _, _ := strings.Cut(line, "=")
		twin, ok := hostTwin(name)
		if !ok || !w.carried[twin] || set[twin] {
			continue
		}
		set[twin] = true
		http.SetCookie(w.ResponseWriter, hostCookieExpiry(twin))
	}
}
