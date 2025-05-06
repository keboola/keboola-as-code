package url

import (
	"net/url"
)

// SanitizeURLString replaces the userinfo part of a URL with "******".
// If the URL is malformed or has no userinfo, it returns the original string.
func SanitizeURLString(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		// Return original if parsing fails
		return rawURL
	}

	// If there's user information, do not return it
	if u.User != nil {
		u.User = nil
		u, err = url.Parse(u.String())
		return u.String()
	}

	// No userinfo, return original
	return rawURL
}
