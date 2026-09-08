package config_test

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestKaiPreviewConfig_Defaults(t *testing.T) {
	t.Parallel()
	cfg := config.New()
	assert.Equal(t, 4*time.Hour, cfg.KaiPreview.SessionTTL)
}

// TestConfig_ConnectionServiceAccountTokenPath checks that the ServiceAccount token path
// defaults to the path mounted by the kbc-stacks chart and can be overridden by an ENV.
func TestConfig_ConnectionServiceAccountTokenPath(t *testing.T) {
	t.Parallel()

	cfg := config.New()
	assert.Equal(t, "/var/run/secrets/connection.keboola.com/serviceaccount/token", cfg.ConnectionServiceAccountTokenPath)

	// Fill in the other required fields, the binding validates the whole configuration.
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

	envs := env.Empty()
	envs.Set("APPS_PROXY_CONNECTION_SERVICE_ACCOUNT_TOKEN_PATH", "/tmp/token")
	require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
		Envs:      envs,
	}, &cfg))
	assert.Equal(t, "/tmp/token", cfg.ConnectionServiceAccountTokenPath)
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
	cfg.SandboxesAPI = config.SandboxesAPI{URL: "https://example"}
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
	cfg.SandboxesAPI = config.SandboxesAPI{URL: "https://example"}
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

func TestSessionsConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := config.New()

	// Empty stream URL means session tracking is off, which is how stacks
	// without Stream deployed stay unaffected.
	assert.Empty(t, cfg.Sessions.StreamURL)

	// A session cookie issued on a websocket handshake is sized to cover the
	// whole connection, so the absolute cap has to be longer than that.
	assert.Greater(t, cfg.Sessions.MaxSessionLength, cfg.Upstream.WsTimeout)
	assert.Greater(t, cfg.Sessions.MaxSessionLength, cfg.Sessions.IdleTimeout)

	assert.Equal(t, 5*time.Minute, cfg.Sessions.HeartbeatInterval)
	assert.Equal(t, 30*time.Minute, cfg.Sessions.IdleTimeout)
	assert.Equal(t, 4096, cfg.Sessions.QueueSize)
	assert.Equal(t, 4, cfg.Sessions.Workers)
	assert.Equal(t, 5*time.Second, cfg.Sessions.SendTimeout)
}

// TestSessionsConfig_Envs pins the ENV names down: they are what the kbc-stacks
// chart sets, and they are documented in docs/apps-proxy/sessions.md.
func TestSessionsConfig_Envs(t *testing.T) {
	t.Parallel()

	cfg := validConfig(t)

	envs := env.Empty()
	envs.Set("APPS_PROXY_SESSIONS_STREAM_URL", "https://stream-in.keboola.local/stream/123/sessions/secret")
	envs.Set("APPS_PROXY_SESSIONS_MAX_SESSION_LENGTH", "8h")
	envs.Set("APPS_PROXY_SESSIONS_HEARTBEAT_INTERVAL", "1m")
	envs.Set("APPS_PROXY_SESSIONS_IDLE_TIMEOUT", "15m")
	envs.Set("APPS_PROXY_SESSIONS_QUEUE_SIZE", "128")
	envs.Set("APPS_PROXY_SESSIONS_WORKERS", "2")
	envs.Set("APPS_PROXY_SESSIONS_SEND_TIMEOUT", "3s")

	require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
		Envs:      envs,
	}, &cfg))

	assert.Equal(t, "https://stream-in.keboola.local/stream/123/sessions/secret", cfg.Sessions.StreamURL)
	assert.Equal(t, 8*time.Hour, cfg.Sessions.MaxSessionLength)
	assert.Equal(t, time.Minute, cfg.Sessions.HeartbeatInterval)
	assert.Equal(t, 15*time.Minute, cfg.Sessions.IdleTimeout)
	assert.Equal(t, 128, cfg.Sessions.QueueSize)
	assert.Equal(t, 2, cfg.Sessions.Workers)
	assert.Equal(t, 3*time.Second, cfg.Sessions.SendTimeout)
}

// validConfig returns a configuration with every required field filled in.
// Binding validates the whole configuration, not just the bound fields.
func validConfig(t *testing.T) config.Config {
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

func TestSessionsConfig_Validate(t *testing.T) {
	t.Parallel()

	// A test that only pins the defaults would not stop an operator from
	// setting a cookie TTL shorter than the idle window, which silently breaks
	// the design: the cookie would expire while the session is still active.
	for name, tc := range map[string]struct {
		maxSessionLength  time.Duration
		heartbeatInterval time.Duration
		idleTimeout       time.Duration
		wantErr           string
	}{
		"defaults are valid": {12 * time.Hour, 5 * time.Minute, 30 * time.Minute, ""},
		"max session length shorter than idle timeout": {
			10 * time.Minute, time.Minute, 30 * time.Minute,
			"sessions.maxSessionLength (10m0s) must be longer than sessions.idleTimeout (30m0s)",
		},
		"max session length equal to idle timeout": {
			30 * time.Minute, time.Minute, 30 * time.Minute,
			"must be longer than sessions.idleTimeout",
		},
		"heartbeat longer than idle timeout": {
			12 * time.Hour, time.Hour, 30 * time.Minute,
			"sessions.heartbeatInterval (1h0m0s) must be shorter than sessions.idleTimeout (30m0s)",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sessions := config.Sessions{
				MaxSessionLength:  tc.maxSessionLength,
				HeartbeatInterval: tc.heartbeatInterval,
				IdleTimeout:       tc.idleTimeout,
				QueueSize:         16,
				Workers:           1,
				SendTimeout:       time.Second,
			}

			err := sessions.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestSessionsConfig_RejectsMalformedStreamURL(t *testing.T) {
	t.Parallel()

	// Without this the proxy starts happily and then fails on every single
	// event, logging the bad URL each time.
	cfg := validConfig(t)
	cfg.Sessions.StreamURL = "not-a-url"
	require.Error(t, configmap.ValidateAndNormalize(&cfg))

	cfg = validConfig(t)
	cfg.Sessions.StreamURL = "https://stream-in.keboola.local/stream/123/sessions/secret"
	require.NoError(t, configmap.ValidateAndNormalize(&cfg))
}

func TestConfig_ValidateWebsocketFitsInSession(t *testing.T) {
	t.Parallel()

	// A cross-struct invariant: it cannot live in Sessions.Validate, which
	// cannot see Upstream. This also proves Config.Validate is reached at all —
	// an unreached hook would enforce nothing.
	cfg := validConfig(t)
	cfg.Sessions.StreamURL = "https://stream-in.keboola.local/stream/123/sessions/secret"
	cfg.Sessions.MaxSessionLength = time.Hour

	err := configmap.ValidateAndNormalize(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be longer than the upstream websocket timeout")

	// The check is scoped to stacks that actually enable tracking.
	cfg = validConfig(t)
	cfg.Sessions.MaxSessionLength = time.Hour
	assert.NoError(t, configmap.ValidateAndNormalize(&cfg))
}
