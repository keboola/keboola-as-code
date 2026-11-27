package upstream

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

func TestRewriteRedirectLocation_PerHost(t *testing.T) {
	t.Parallel()

	publicURL := &url.URL{Scheme: "https", Host: "hub.example.com"}
	target := &url.URL{Scheme: "http", Host: "app-123.svc:8888"}
	slug := "my-app"

	newUpstream := func(customHost string) *AppUpstream {
		app := api.AppConfig{ID: "123", AppSlug: &slug, CustomHost: customHost}
		return &AppUpstream{app: app, target: target, baseURL: app.BaseURL(publicURL)}
	}

	for _, tc := range []struct{ name, customHost, origin string }{
		{"hub", "", "https://my-app-123.hub.example.com"},
		{"custom", "data.customer.org", "https://data.customer.org"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := &http.Response{Header: http.Header{}}
			res.Header.Set("Location", "http://app-123.svc:8888/login?x=1")
			res.Header.Set("Content-Location", "http://app-123.svc:8888/doc")
			require.NoError(t, newUpstream(tc.customHost).rewriteRedirectLocation(res))
			assert.Equal(t, tc.origin+"/login?x=1", res.Header.Get("Location"))
			assert.Equal(t, tc.origin+"/doc", res.Header.Get("Content-Location"))
		})
	}

	t.Run("third party untouched", func(t *testing.T) {
		t.Parallel()
		res := &http.Response{Header: http.Header{}}
		res.Header.Set("Location", "https://idp.example.net/authorize")
		require.NoError(t, newUpstream("data.customer.org").rewriteRedirectLocation(res))
		assert.Equal(t, "https://idp.example.net/authorize", res.Header.Get("Location"))
	})
}
