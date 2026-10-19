package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestAppUserTokenConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	assert.Equal(t, 5*time.Minute, cfg.AppUserToken.TTL)
	assert.Empty(t, cfg.AppUserToken.SigningKeys)
	assert.Empty(t, cfg.AppUserToken.ActiveKid)

	valid := requiredConfig(t)
	require.NoError(t, configmap.ValidateAndNormalize(&valid))
}

// TestAppUserTokenConfig_Envs pins the ENV names down: they are what the kbc-stacks chart sets.
func TestAppUserTokenConfig_Envs(t *testing.T) {
	t.Parallel()

	cfg := requiredConfig(t)

	envs := env.Empty()
	envs.Set("APPS_PROXY_APP_USER_TOKEN_SIGNING_KEYS", `[{"kid":"k1","pem":"x"}]`)
	envs.Set("APPS_PROXY_APP_USER_TOKEN_ACTIVE_KID", "k1")
	envs.Set("APPS_PROXY_APP_USER_TOKEN_TTL", "2m")
	require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
		Envs:      envs,
	}, &cfg))

	assert.JSONEq(t, `[{"kid":"k1","pem":"x"}]`, cfg.AppUserToken.SigningKeys)
	assert.Equal(t, "k1", cfg.AppUserToken.ActiveKid)
	assert.Equal(t, 2*time.Minute, cfg.AppUserToken.TTL)
}

func TestAppUserTokenConfig_Validate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(c *config.AppUserToken)
		wantErr string
	}{
		{name: "ttl over 5 minutes", mutate: func(c *config.AppUserToken) { c.TTL = 6 * time.Minute }, wantErr: "appUserToken.ttl"},
		{name: "ttl negative", mutate: func(c *config.AppUserToken) { c.TTL = -time.Minute }, wantErr: "appUserToken.ttl"},
		{name: "ttl under 1 second", mutate: func(c *config.AppUserToken) { c.TTL = time.Millisecond }, wantErr: "appUserToken.ttl"},
		{name: "ttl zero", mutate: func(c *config.AppUserToken) { c.TTL = 0 }, wantErr: "appUserToken.ttl"},
		{name: "keys without active kid", mutate: func(c *config.AppUserToken) { c.SigningKeys = "[]" }, wantErr: "appUserToken.signingKeys and appUserToken.activeKid must be set together"},
		{name: "active kid without keys", mutate: func(c *config.AppUserToken) { c.ActiveKid = "k1" }, wantErr: "appUserToken.signingKeys and appUserToken.activeKid must be set together"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := requiredConfig(t)
			tc.mutate(&cfg.AppUserToken)
			err := configmap.ValidateAndNormalize(&cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestAppUserTokenConfig_ValidateAllowsBoth(t *testing.T) {
	t.Parallel()

	cfg := requiredConfig(t)
	cfg.AppUserToken.SigningKeys = `[{"kid":"k1","pem":"x"}]`
	cfg.AppUserToken.ActiveKid = "k1"
	cfg.AppUserToken.TTL = 5 * time.Minute
	require.NoError(t, configmap.ValidateAndNormalize(&cfg))
}
