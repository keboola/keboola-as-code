package http

import (
	"net/url"
	"strings"
)

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
