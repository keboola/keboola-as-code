package config_test

import (
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
