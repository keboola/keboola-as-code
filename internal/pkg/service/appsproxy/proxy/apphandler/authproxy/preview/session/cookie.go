package session

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/requestcookie"
)

const CookieName = "__Host-kbc-app-preview-session"

func ClearCookie() *http.Cookie {
	return &http.Cookie{ //nolint:gosec // G124: SameSite=None with Partitioned is required for the cross-site embedded preview
		Name:        CookieName,
		Path:        "/",
		MaxAge:      -1,
		Secure:      true,
		HttpOnly:    true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	}
}

func TakeCookie(req *http.Request) string {
	value := ""
	if c, err := req.Cookie(CookieName); err == nil {
		value = c.Value
	}
	requestcookie.Remove(req, func(name string) bool { return name == CookieName })
	return value
}
