package config_test

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func requiredConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.New()
	cfg.CookieSecretSalt = "x"
	cfg.CsrfTokenSalt = "x"
	cfg.SandboxesAPI.URL = "https://example"
	cfg.K8s = config.K8s{AppsNamespace: "ns"}
	storageURL, err := url.Parse("https://connection.keboola.com")
	require.NoError(t, err)
	cfg.StorageAPIURL = storageURL
	cfg.KaiPreview = config.KaiPreview{
		HandshakeSigningKey: "k1",
		SessionSigningKey:   "k2",
		SessionTTL:          4 * time.Hour,
		AllowedOrigins:      []string{"https://connection.keboola.com"},
	}
	return cfg
}

func TestPreviewConfig_Defaults(t *testing.T) {
	t.Parallel()
	cfg := config.New()
	assert.Empty(t, cfg.Preview.JWKSURL)
	assert.False(t, cfg.Preview.Enabled())
}

func TestPreviewConfig_DisabledNeedsNothing(t *testing.T) {
	t.Parallel()
	cfg := requiredConfig(t)
	require.NoError(t, configmap.ValidateAndNormalize(&cfg))
}

func TestPreviewConfig_EnvNames(t *testing.T) {
	t.Parallel()
	cfg := requiredConfig(t)

	envs := env.Empty()
	envs.Set("APPS_PROXY_PREVIEW_JWKS_URL", "http://sandboxes-service-api.default.svc.cluster.local/.well-known/jwks.json")
	envs.Set("APPS_PROXY_PREVIEW_ISSUER", "https://apps.keboola.com")
	envs.Set("APPS_PROXY_PREVIEW_SESSION_SIGNING_KEY", strings.Repeat("k", 64))
	envs.Set("APPS_PROXY_PREVIEW_ALLOWED_FRAME_ANCESTORS", "https://connection.keboola.com,https://connection.north-europe.azure.keboola.com/")
	require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
		Envs:      envs,
	}, &cfg))

	assert.True(t, cfg.Preview.Enabled())
	assert.Equal(t, "http://sandboxes-service-api.default.svc.cluster.local/.well-known/jwks.json", cfg.Preview.JWKSURL)
	assert.Equal(t, "https://apps.keboola.com", cfg.Preview.Issuer)
	assert.Equal(t, strings.Repeat("k", 64), cfg.Preview.SessionSigningKey)
	assert.Equal(t, []string{"https://connection.keboola.com", "https://connection.north-europe.azure.keboola.com"}, cfg.Preview.AllowedFrameAncestors)
}

func TestPreviewConfig_EnabledValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(p *config.Preview)
		wantErr string
	}{
		{name: "missing-issuer", mutate: func(p *config.Preview) { p.Issuer = "" }, wantErr: "preview.issuer"},
		{name: "short-key", mutate: func(p *config.Preview) { p.SessionSigningKey = "short" }, wantErr: "preview.sessionSigningKey"},
		{name: "ancestor-with-path", mutate: func(p *config.Preview) { p.AllowedFrameAncestors = []string{"https://connection.keboola.com/admin"} }, wantErr: "preview.allowedFrameAncestors"},
		{name: "ancestor-with-semicolon", mutate: func(p *config.Preview) { p.AllowedFrameAncestors = []string{"https://a.com;script-src *"} }, wantErr: "preview.allowedFrameAncestors"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := requiredConfig(t)
			cfg.Preview.JWKSURL = "http://sandboxes-service-api.default.svc.cluster.local/.well-known/jwks.json"
			cfg.Preview.Issuer = "https://apps.keboola.com"
			cfg.Preview.SessionSigningKey = strings.Repeat("k", 64)
			tc.mutate(&cfg.Preview)
			err := configmap.ValidateAndNormalize(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
