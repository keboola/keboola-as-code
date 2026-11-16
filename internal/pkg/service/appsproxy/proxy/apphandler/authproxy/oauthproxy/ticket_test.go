package oauthproxy

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTicketKey(t *testing.T) {
	t.Parallel()

	cookie := options.Cookie{Name: redisSessionCookieName, Secret: "0123456789abcdef0123456789abcdef", Expire: 24 * time.Hour}
	id := redisSessionCookieName + "-0011aabb"
	secret := base64.RawURLEncoding.EncodeToString([]byte("ticket-secret-16"))
	v2 := "v2." + base64.RawURLEncoding.EncodeToString([]byte(id)) + "." + secret

	sign := func(t *testing.T, value string, at time.Time, seed string) *http.Cookie {
		t.Helper()
		signed, err := encryption.SignedValue(seed, cookie.Name, []byte(value), at)
		require.NoError(t, err)
		return &http.Cookie{Name: cookie.Name, Value: signed}
	}

	cases := []struct {
		name   string
		cookie func(t *testing.T) *http.Cookie
		want   string
		wantOK bool
	}{
		{name: "valid v2 ticket", cookie: func(t *testing.T) *http.Cookie { return sign(t, v2, time.Now(), cookie.Secret) }, want: id, wantOK: true},
		{name: "bad signature", cookie: func(t *testing.T) *http.Cookie { return sign(t, v2, time.Now(), "another-secret-another-secret-00") }},
		{name: "older than the expiry", cookie: func(t *testing.T) *http.Cookie { return sign(t, v2, time.Now().Add(-25*time.Hour), cookie.Secret) }},
		{name: "old ticket format", cookie: func(t *testing.T) *http.Cookie { return sign(t, id+"."+secret, time.Now(), cookie.Secret) }},
		{name: "not base64 id", cookie: func(t *testing.T) *http.Cookie { return sign(t, "v2.!!!."+secret, time.Now(), cookie.Secret) }},
		{name: "no cookie"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.hub.keboola.local/ws", nil)
			if tc.cookie != nil {
				req.AddCookie(tc.cookie(t))
			}
			got, ok := ticketKey(req, cookie)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, secret)
		})
	}
}
