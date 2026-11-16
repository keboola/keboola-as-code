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

	otherSeed := "another-secret-another-secret-00"
	cases := []struct {
		name   string
		value  string
		seed   string
		age    time.Duration
		want   string
		wantOK bool
	}{
		{name: "valid v2 ticket", value: v2, want: id, wantOK: true},
		{name: "bad signature", value: v2, seed: otherSeed},
		{name: "older than the expiry", value: v2, age: 25 * time.Hour},
		{name: "old ticket format", value: id + "." + secret},
		{name: "not base64 id", value: "v2.!!!." + secret},
		{name: "no cookie"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.hub.keboola.local/ws", nil)
			if tc.value != "" {
				seed := cookie.Secret
				if tc.seed != "" {
					seed = tc.seed
				}
				signed, err := encryption.SignedValue(seed, cookie.Name, []byte(tc.value), time.Now().Add(-tc.age))
				require.NoError(t, err)
				req.AddCookie(&http.Cookie{Name: cookie.Name, Value: signed})
			}
			got, ok := ticketKey(req, cookie)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, secret)
		})
	}
}
