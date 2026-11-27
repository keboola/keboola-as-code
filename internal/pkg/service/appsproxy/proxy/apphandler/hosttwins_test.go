package apphandler

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTwinExpiry(t *testing.T) {
	t.Parallel()

	pairs := []struct{ legacy, host string }{
		{"_kbc_dasid", "__Host-kbc-dasid"},
		{"proxyBasicAuth", "__Host-kbc-basic-auth"},
		{"_oauth2_provider", "__Host-kbc-auth-provider"},
		{"_oauth2_proxy_ticket", "__Host-kbc-oauth2-ticket"},
		{"_oauth2_proxy_ticket_csrf", "__Host-kbc-oauth2-ticket_csrf"},
	}
	for _, p := range pairs {
		t.Run(p.legacy, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.example.com/", nil)
			req.AddCookie(&http.Cookie{Name: p.host, Value: "v"})
			rec := httptest.NewRecorder()
			w := withTwinExpiry(rec, req)

			http.SetCookie(w, &http.Cookie{Name: p.legacy, Value: "x", Path: "/"})
			w.WriteHeader(http.StatusOK)

			var expired *http.Cookie
			for _, c := range rec.Result().Cookies() {
				if c.Name == p.host {
					expired = c
				}
			}
			require.NotNil(t, expired)
			assert.Negative(t, expired.MaxAge)
			assert.Empty(t, expired.Domain)
			assert.True(t, expired.Secure)
		})
	}

	t.Run("not carried, not expired", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.example.com/", nil)
		rec := httptest.NewRecorder()
		assert.Same(t, rec, withTwinExpiry(rec, req), "no wrapper without host-only cookies")
	})

	t.Run("carried twin of a cookie that is not written", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.example.com/", nil)
		req.AddCookie(&http.Cookie{Name: "__Host-kbc-oauth2-ticket", Value: "v"})
		rec := httptest.NewRecorder()
		w := withTwinExpiry(rec, req)
		w.WriteHeader(http.StatusOK)
		assert.Empty(t, rec.Result().Cookies())
	})
}

type hijackRecorder struct {
	*httptest.ResponseRecorder
	conn net.Conn
}

func (h *hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return h.conn, bufio.NewReadWriter(bufio.NewReader(h.conn), bufio.NewWriter(h.conn)), nil
}

func TestTwinExpiry_ExpiresOnHijack(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.example.com/", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-kbc-oauth2-ticket", Value: "v"})
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	rec := &hijackRecorder{ResponseRecorder: httptest.NewRecorder(), conn: server}
	w := withTwinExpiry(rec, req)

	http.SetCookie(w, &http.Cookie{Name: "_oauth2_proxy_ticket", Value: "x", Path: "/"})
	hijacker, ok := w.(http.Hijacker)
	require.True(t, ok)
	_, _, err := hijacker.Hijack()
	require.NoError(t, err)

	lines := rec.Header().Values("Set-Cookie")
	require.Len(t, lines, 2, "a 101 is written from the header map, the twin must be in it")
	assert.Contains(t, lines[1], "__Host-kbc-oauth2-ticket=")
}
