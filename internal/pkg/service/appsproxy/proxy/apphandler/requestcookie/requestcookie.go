package requestcookie

import (
	"net/http"
	"strings"
)

// Remove deletes from the request's Cookie headers every cookie whose name drop matches.
// A line with no matching cookie is kept unchanged.
func Remove(req *http.Request, drop func(name string) bool) {
	lines := req.Header.Values("Cookie")
	if len(lines) == 0 {
		return
	}
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if l := withoutCookies(line, drop); l != "" {
			kept = append(kept, l)
		}
	}
	req.Header.Del("Cookie")
	for _, l := range kept {
		req.Header.Add("Cookie", l)
	}
}

func withoutCookies(line string, drop func(name string) bool) string {
	parts := strings.Split(line, ";")
	kept := make([]string, 0, len(parts))
	dropped := false
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if name, _, _ := strings.Cut(p, "="); drop(strings.TrimSpace(name)) {
			dropped = true
			continue
		}
		kept = append(kept, p)
	}
	if !dropped {
		return line
	}
	return strings.Join(kept, "; ")
}
