package session

import (
	"net/http"
	"strings"
)

const CookieName = "__Host-kbc-app-preview-session"

func ClearCookie() *http.Cookie {
	return &http.Cookie{
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
	lines := req.Header.Values("Cookie")
	if len(lines) == 0 {
		return value
	}
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if l := withoutCookie(line, CookieName); l != "" {
			kept = append(kept, l)
		}
	}
	req.Header.Del("Cookie")
	for _, l := range kept {
		req.Header.Add("Cookie", l)
	}
	return value
}

func withoutCookie(line, name string) string {
	parts := strings.Split(line, ";")
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		cookieName, _, _ := strings.Cut(p, "=")
		if p == "" || strings.TrimSpace(cookieName) == name {
			continue
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, "; ")
}
