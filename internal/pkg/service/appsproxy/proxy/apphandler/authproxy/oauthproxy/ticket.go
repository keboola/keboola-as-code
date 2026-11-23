package oauthproxy

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
)

const ticketVersion = "v2"

// ticketKey returns the session store key of the request's ticket cookie, never its secret.
func ticketKey(req *http.Request, cookie options.Cookie) (string, bool) {
	c, err := req.Cookie(cookie.Name)
	if err != nil {
		return "", false
	}
	secret, err := cookie.GetSecret()
	if err != nil {
		return "", false
	}
	value, _, ok := encryption.Validate(c, secret, cookie.Expire)
	if !ok {
		return "", false
	}
	parts := strings.Split(string(value), ".")
	if len(parts) != 3 || parts[0] != ticketVersion {
		return "", false
	}
	id, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(id) == 0 {
		return "", false
	}
	return string(id), true
}
