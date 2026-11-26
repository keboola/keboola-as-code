package upstream

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRewriteWebsocketOrigin(t *testing.T) {
	t.Parallel()

	const (
		upstreamOrigin = "http://app-123.svc.cluster.local:8888"
		hubHost        = "mydata-123.hub.keboola.test"
		customHost     = "reports.customer.test"
	)

	newUpstream := func() *AppUpstream {
		return &AppUpstream{
			target:  &url.URL{Scheme: "http", Host: "app-123.svc.cluster.local:8888"},
			baseURL: &url.URL{Scheme: "https", Host: hubHost, Path: "/"},
		}
	}

	testCases := []struct {
		name           string
		host           string
		origin         string // empty = header absent
		forwardedHost  string
		expectedOrigin string
	}{
		{
			name:           "own origin is rewritten to upstream",
			host:           hubHost,
			origin:         "https://" + hubHost,
			expectedOrigin: upstreamOrigin,
		},
		{
			name:           "own origin on a custom domain is rewritten to upstream",
			host:           customHost,
			origin:         "https://" + customHost,
			expectedOrigin: upstreamOrigin,
		},
		{
			name:           "foreign origin is passed through",
			host:           hubHost,
			origin:         "https://evil.example",
			expectedOrigin: "https://evil.example",
		},
		{
			name:           "other app host is passed through",
			host:           hubHost,
			origin:         "https://" + customHost,
			expectedOrigin: "https://" + customHost,
		},
		{
			name:           "spoofed X-Forwarded-Host does not make a foreign origin pass",
			host:           hubHost,
			origin:         "https://evil.example",
			forwardedHost:  "evil.example",
			expectedOrigin: "https://evil.example",
		},
		{
			name:           "wrong scheme is passed through",
			host:           hubHost,
			origin:         "http://" + hubHost,
			expectedOrigin: "http://" + hubHost,
		},
		{
			name:           "missing origin is set to upstream",
			host:           hubHost,
			origin:         "",
			expectedOrigin: upstreamOrigin,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &http.Request{Host: tc.host, Header: http.Header{}}
			out := &http.Request{Host: tc.host, Header: http.Header{}}
			if tc.origin != "" {
				in.Header.Set("Origin", tc.origin)
				out.Header.Set("Origin", tc.origin)
			}
			if tc.forwardedHost != "" {
				in.Header.Set("X-Forwarded-Host", tc.forwardedHost)
				out.Header.Set("X-Forwarded-Host", tc.forwardedHost)
			}

			newUpstream().rewriteWebsocketOrigin(&httputil.ProxyRequest{In: in, Out: out})

			assert.Equal(t, tc.expectedOrigin, out.Header.Get("Origin"))
		})
	}
}
