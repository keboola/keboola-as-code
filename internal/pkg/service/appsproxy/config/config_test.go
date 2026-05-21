package config_test

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestKaiPreviewConfig_Defaults(t *testing.T) {
	t.Parallel()
	cfg := config.New()
	assert.Equal(t, 4*time.Hour, cfg.KaiPreview.SessionTTL)
}

func TestKaiPreviewConfig_RequiresSigningKeys(t *testing.T) {
	t.Parallel()
	cfg := config.New()
	cfg.CookieSecretSalt = "x"
	cfg.CsrfTokenSalt = "x"
	// KaiPreview signing keys intentionally empty
	err := configmap.ValidateAndNormalize(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kaiPreview.handshakeSigningKey")
	assert.Contains(t, err.Error(), "kaiPreview.sessionSigningKey")
}

func TestKaiPreviewConfig_NormalizeStripsTrailingSlash(t *testing.T) {
	t.Parallel()
	cfg := config.New()
	cfg.CookieSecretSalt = "x"
	cfg.CsrfTokenSalt = "x"
	cfg.SandboxesAPI = config.SandboxesAPI{URL: "https://example", Token: "t"}
	cfg.K8s = config.K8s{AppsNamespace: "ns"}
	storageURL, _ := url.Parse("https://connection.keboola.com")
	cfg.StorageAPIURL = storageURL
	cfg.KaiPreview = config.KaiPreview{
		HandshakeSigningKey: "k1",
		SessionSigningKey:   "k2",
		SessionTTL:          4 * time.Hour,
		AllowedOrigins:      []string{"https://connection.keboola.com/", "https://staging.keboola.com"},
	}
	err := configmap.ValidateAndNormalize(&cfg)
	require.NoError(t, err)
	assert.Equal(t, "https://connection.keboola.com", cfg.KaiPreview.AllowedOrigins[0])
	assert.Equal(t, "https://staging.keboola.com", cfg.KaiPreview.AllowedOrigins[1])
}

func TestConfig_RequiresStorageAPIURL(t *testing.T) {
	t.Parallel()
	cfg := config.New()
	assert.Nil(t, cfg.StorageAPIURL, "default config must NOT set StorageAPIURL — operators must configure it per stack")

	// Even with everything else valid, missing StorageAPIURL must fail validation.
	cfg.CookieSecretSalt = "x"
	cfg.CsrfTokenSalt = "x"
	cfg.SandboxesAPI = config.SandboxesAPI{URL: "https://example", Token: "t"}
	cfg.K8s = config.K8s{AppsNamespace: "ns"}
	cfg.KaiPreview = config.KaiPreview{
		HandshakeSigningKey: "k1",
		SessionSigningKey:   "k2",
		SessionTTL:          4 * time.Hour,
		AllowedOrigins:      []string{"https://connection.keboola.com"},
	}
	err := configmap.ValidateAndNormalize(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storageApiUrl")
}
