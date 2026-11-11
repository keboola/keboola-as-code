package apphandler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

func TestIsProxyCookie(t *testing.T) {
	t.Parallel()

	removed := []string{
		"_oauth2_proxy",
		"_oauth2_proxy_0",
		"_oauth2_proxy_1",
		"_oauth2_proxy_10",
		"_oauth2_proxy_csrf",
		"_oauth2_proxy_x_csrf",
		"_oauth2_proxy_",
		"_oauth2_proxy_x",
		"_oauth2_proxy_0x",
		"_oauth2_proxy_csrf_0",
		"_oauth2_provider",
		"_kbc_dasid",
		"proxyBasicAuth",
		"__Host-kbc-app-preview-session",
	}
	for _, name := range removed {
		assert.True(t, isProxyCookie(name), name)
	}

	kept := []string{
		"_streamlit_xsrf",
		"_oauth2_proxyx",
		"x_oauth2_proxy",
		"_kbc_dasid_0",
		"proxybasicauth",
		"session",
	}
	for _, name := range kept {
		assert.False(t, isProxyCookie(name), name)
	}
}

func TestStripProxyCookies(t *testing.T) {
	t.Parallel()

	var received *http.Request
	next := chain.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) error {
		received = req
		return nil
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.hub.keboola.local/", nil)
	req.Header.Add("Cookie", "_oauth2_proxy_0=a; _streamlit_xsrf=2|x|y; _oauth2_proxy_1=b")
	req.Header.Add("Cookie", "_kbc_dasid=c")
	require.NoError(t, stripProxyCookies(next).ServeHTTPOrError(httptest.NewRecorder(), req))

	assert.Equal(t, []string{"_streamlit_xsrf=2|x|y"}, received.Header.Values("Cookie"))
	assert.Len(t, req.Header.Values("Cookie"), 2, "the caller's request is not modified")
}
