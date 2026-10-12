package transport

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckPublicAddress(t *testing.T) {
	t.Parallel()

	rejected := []string{
		"127.0.0.1:80",
		"[::1]:443",
		"169.254.169.254:80",
		"[fe80::1]:80",
		"10.0.0.1:443",
		"172.16.0.1:443",
		"172.31.255.255:443",
		"192.168.1.1:443",
		"100.64.0.1:443",
		"[fd00::1]:443",
		"0.0.0.0:80",
		"[::]:80",
		"224.0.0.1:80",
		"[ff02::1]:80",
		"[::ffff:127.0.0.1]:80",
		"[::ffff:169.254.169.254]:80",
		"not-an-address:80",
	}
	for _, address := range rejected {
		require.Error(t, checkPublicAddress("tcp", address, nil), address)
	}

	allowed := []string{
		"8.8.8.8:443",
		"172.32.0.1:443",
		"100.128.0.1:443",
		"[2001:4860:4860::8888]:443",
	}
	for _, address := range allowed {
		require.NoError(t, checkPublicAddress("tcp", address, nil), address)
	}
}

func TestPublicOnlyHTTPClient_RefusesLoopback(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
	}))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := NewPublicOnlyHTTPClient("test-agent").Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not allowed")
	assert.False(t, called.Load())
}

func TestPublicOnlyHTTPClient_IgnoresProxyEnvironment(t *testing.T) {
	t.Parallel()

	client := NewPublicOnlyHTTPClient("test-agent")
	ua, ok := client.Transport.(*userAgentTransport)
	require.True(t, ok)
	httpTransport, ok := ua.next.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, httpTransport.Proxy)
}

func TestPublicOnlyHTTPClient_SetsDefaultUserAgent(t *testing.T) {
	t.Parallel()

	var got string
	next := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		got = req.Header.Get("User-Agent")
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	rt := &userAgentTransport{next: next, userAgent: "test-agent"}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "test-agent", got)

	req.Header.Set("User-Agent", "custom")
	resp, err = rt.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "custom", got)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
