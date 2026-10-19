package proxy_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
)

const jwksPath = "/.well-known/jwks.json"

func appTokenSigningKeys(t *testing.T, kids ...string) string {
	t.Helper()
	type entry struct {
		Kid string `json:"kid"`
		Pem string `json:"pem"`
	}
	entries := make([]entry, 0, len(kids))
	for _, kid := range kids {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		der, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		entries = append(entries, entry{Kid: kid, Pem: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))})
	}
	out, err := json.Marshal(entries)
	require.NoError(t, err)
	return string(out)
}

func withAppUserTokenKeys(keys, activeKid string) func(cfg *config.Config, _ *previewtest.JWKSServer) {
	return func(cfg *config.Config, _ *previewtest.JWKSServer) {
		cfg.AppUserToken.SigningKeys = keys
		cfg.AppUserToken.ActiveKid = activeKid
	}
}

func TestAppUserToken_JWKSEndpoint(t *testing.T) {
	t.Parallel()

	t.Run("keys configured", func(t *testing.T) {
		t.Parallel()
		keys := appTokenSigningKeys(t, "k1", "k2")
		env := startPreviewEnv(t, withAppUserTokenKeys(keys, "k2"))
		keyring, err := apptoken.NewKeyring(keys, "k2")
		require.NoError(t, err)

		resp := env.get("hub.keboola.local", jwksPath, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/jwk-set+json", resp.Header.Get("Content-Type"))
		assert.Equal(t, "public, max-age=300", resp.Header.Get("Cache-Control"))
		assert.Equal(t, string(keyring.JWKS()), readBody(t, resp))
	})

	t.Run("no keys", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t)

		resp := env.get("hub.keboola.local", jwksPath, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/jwk-set+json", resp.Header.Get("Content-Type"))
		assert.Equal(t, "public, max-age=300", resp.Header.Get("Cache-Control"))
		assert.JSONEq(t, `{"keys":[]}`, readBody(t, resp))
	})

	t.Run("app subdomain reaches the app", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))

		resp := env.get(devmodeHost, jwksPath, nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "Hello, client", readBody(t, resp))
		requests := *env.appServer.Requests
		require.NotEmpty(t, requests)
		assert.Equal(t, jwksPath, requests[len(requests)-1].URL.Path)
	})
}
