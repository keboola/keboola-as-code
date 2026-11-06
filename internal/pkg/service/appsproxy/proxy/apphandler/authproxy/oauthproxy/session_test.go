package oauthproxy

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

func TestProxyConfig_SessionLifetime(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = "salt"

	app := api.AppConfig{ID: "123", ProjectID: "456", AppSlug: new("app")}
	auth := provider.GitHub{
		Base:         provider.Base{Info: provider.Info{ID: "github", Type: provider.TypeGitHub}},
		ClientID:     "client",
		ClientSecret: "secret",
	}

	v, err := proxyConfig(t.Context(), cfg, scopeResolver{}, nil, nil, app, auth, nil)
	require.NoError(t, err)
	assert.Equal(t, time.Hour, v.Cookie.Refresh)
	assert.Equal(t, 24*time.Hour, v.Cookie.Expire)
}
