package oauthproxy

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

func TestCallbackURL(t *testing.T) {
	t.Parallel()

	publicURL := &url.URL{Scheme: "https", Host: "hub.example.com"}
	slug := "my-app"

	hub := api.AppConfig{ID: "123", AppSlug: &slug}
	assert.Equal(t, "https://my-app-123.hub.example.com/_proxy/callback", callbackURL(hub, publicURL))

	custom := hub
	custom.CustomHost = "data.customer.org"
	assert.Equal(t, "https://data.customer.org/_proxy/callback", callbackURL(custom, publicURL))
}

func TestCookieDomains(t *testing.T) {
	t.Parallel()

	publicURL := &url.URL{Scheme: "https", Host: "hub.example.com"}
	slug := "my-app"
	hub := api.AppConfig{ID: "123", AppSlug: &slug}
	custom := hub
	custom.CustomHost = "data.customer.org"

	assert.Equal(t, []string{"my-app-123.hub.example.com"}, cookieDomains(hub, publicURL))
	assert.Empty(t, cookieDomains(custom, publicURL), "a custom host gets host-only cookies")
}
