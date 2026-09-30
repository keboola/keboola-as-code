// Package preview implements dev-mode app preview links: a link token minted by
// sandboxes-service is redeemed on the app host for a host-only session cookie.
package preview

import (
	"net/url"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func NormalizeOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", errors.Errorf("invalid origin: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "https" && scheme != "http" {
		return "", errors.Errorf(`invalid origin scheme "%s"`, u.Scheme)
	}
	if u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("origin must consist of a scheme and a host only")
	}
	port := u.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	b.WriteString(strings.ToLower(u.Hostname()))
	if port != "" {
		b.WriteByte(':')
		b.WriteString(port)
	}
	return b.String(), nil
}
