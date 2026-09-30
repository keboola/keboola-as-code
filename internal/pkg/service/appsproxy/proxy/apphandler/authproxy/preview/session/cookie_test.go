package session_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
)

func TestClearCookie(t *testing.T) {
	t.Parallel()
	c := session.ClearCookie()
	assert.Equal(t, session.CookieName, c.Name)
	assert.Equal(t, "/", c.Path)
	assert.Negative(t, c.MaxAge)
	assert.True(t, c.Secure)
	assert.True(t, c.Partitioned)
}

func TestTakeCookie(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-123.hub.keboola.local/", nil)
	req.Header.Add("Cookie", "a=1; __Host-kbc-app-preview-session=secret; b=2")
	req.Header.Add("Cookie", "__Host-kbc-app-preview-session=dup")
	req.Header.Add("Cookie", "c=3;")

	assert.Equal(t, "secret", session.TakeCookie(req))
	assert.Equal(t, []string{"a=1; b=2", "c=3"}, req.Header.Values("Cookie"))
	_, err := req.Cookie(session.CookieName)
	require.ErrorIs(t, err, http.ErrNoCookie)

	empty := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-123.hub.keboola.local/", nil)
	assert.Empty(t, session.TakeCookie(empty))
	assert.Empty(t, empty.Header.Values("Cookie"))
}

func TestTakeCookie_TrailingWhitespaceInName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"__Host-kbc-app-preview-session space",
		"__Host-kbc-app-preview-session tab",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			raw := "__Host-kbc-app-preview-session =v"
			if name == "__Host-kbc-app-preview-session tab" {
				raw = "__Host-kbc-app-preview-session\t=v"
			}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-123.hub.keboola.local/", nil)
			req.Header.Add("Cookie", "a=1; "+raw+"; b=2")

			assert.Equal(t, "v", session.TakeCookie(req))
			assert.Equal(t, []string{"a=1; b=2"}, req.Header.Values("Cookie"))
		})
	}
}
