package api_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

func TestAppConfig_PublicOrigin(t *testing.T) {
	t.Parallel()

	publicURL := &url.URL{Scheme: "https", Host: "hub.example.com"}
	slug := "my-app"
	app := api.AppConfig{ID: "123", AppSlug: &slug}

	t.Run("hub host", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "https://my-app-123.hub.example.com/", app.BaseURL(publicURL).String())
		assert.Equal(t, "my-app-123.hub.example.com", app.CookieDomain(publicURL))
	})

	t.Run("custom host", func(t *testing.T) {
		t.Parallel()
		custom := app
		custom.CustomHost = "data.customer.org"
		assert.Equal(t, "https://data.customer.org/", custom.BaseURL(publicURL).String())
		assert.Equal(t, "data.customer.org", custom.CookieDomain(publicURL))
	})

	t.Run("custom host takes the scheme from the public URL", func(t *testing.T) {
		t.Parallel()
		custom := app
		custom.CustomHost = "data.customer.org"
		assert.Equal(t, "http://data.customer.org/", custom.BaseURL(&url.URL{Scheme: "http", Host: "hub.local:8080"}).String())
	})
}
