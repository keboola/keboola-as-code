package proxy_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
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

const (
	oidcOrigin       = "https://" + oidcHost
	prefixHost       = "prefix.hub.keboola.local"
	oidcSandbox      = "app-oidc-sbx"
	oidcWorkspaceID  = "91011"
	emailVerifiedHdr = "X-Kbc-User-Email-Verified"
)

var oidcRef = k8sapp.WorkloadRef{AppID: "oidc"} //nolint:gochecknoglobals // test fixture

// withOIDCWorkspace gives the "oidc" app a production Sandbox with a workspace.
// The Sandbox is created through the client: initial objects of kind Sandbox
// land under a guessed resource name the informer never lists.
func (e *previewEnv) withOIDCWorkspace() {
	e.t.Helper()
	sandbox := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.SandboxVersion,
		"kind":       "Sandbox",
		"metadata":   map[string]any{"name": oidcSandbox, "namespace": "keboola"},
		"spec":       map[string]any{"appId": "oidc"},
		"status": map[string]any{
			"currentState": string(k8sapp.AppActualStateRunning),
			"workspace":    map[string]any{"id": oidcWorkspaceID, "name": "Workspace"},
		},
	}}
	client := e.mocked.TestFakeK8sClient()
	_, err := client.Resource(k8sapp.SandboxGVR()).Namespace("keboola").Create(e.t.Context(), sandbox, metav1.CreateOptions{})
	require.NoError(e.t, err)
	patch := []byte(`{"status":{"productionSandbox":"` + oidcSandbox + `"}}`)
	_, err = client.Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(e.t.Context(), "app-oidc", k8stypes.MergePatchType, patch, metav1.PatchOptions{})
	require.NoError(e.t, err)
	require.Eventually(e.t, func() bool {
		info, ok := e.d.AppStateWatcher().GetState(e.t.Context(), oidcRef)
		return ok && info.WorkspaceID == oidcWorkspaceID
	}, 5*time.Second, 50*time.Millisecond)
}

// oidcLogin runs the OAuth flow against mockoidc and returns a client holding
// the oauth2-proxy session cookie for the host.
func (e *previewEnv) oidcLogin(provider *mockoidc.MockOIDC, user *mockoidc.MockUser, startURL string) *http.Client {
	e.t.Helper()
	jar, err := cookiejar.New(nil)
	require.NoError(e.t, err)
	client := *e.client
	client.Jar = jar

	provider.QueueUser(user)
	resp := e.send(&client, startURL)
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	resp = e.send(&client, resp.Header.Get("Location"))
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	resp = e.send(&client, resp.Header.Get("Location"))
	require.Equal(e.t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	resp = e.send(&client, extractMetaRefreshTag(e.t, body))
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	return &client
}

// syncClock moves the fake clock to the wall clock. Cookies the proxy sets
// expire relative to it, and the cookie jar drops any already in the past.
func (e *previewEnv) syncClock() {
	e.clock.Advance(time.Since(e.clock.Now()))
}

func (e *previewEnv) send(client *http.Client, target string, headers ...string) *http.Response {
	e.t.Helper()
	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodGet, target, nil)
	require.NoError(e.t, err)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := client.Do(req)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (e *previewEnv) lastAppRequest() *http.Request {
	e.t.Helper()
	requests := *e.appServer.Requests
	require.NotEmpty(e.t, requests)
	return requests[len(requests)-1]
}

type verifiedToken struct {
	header  map[string]any
	payload map[string]any
}

// verify checks the token against the keys served at the hub JWKS endpoint.
func (e *previewEnv) verify(token string) verifiedToken {
	e.t.Helper()
	var set jose.JSONWebKeySet
	require.NoError(e.t, json.Unmarshal([]byte(readBody(e.t, e.get("hub.keboola.local", jwksPath, nil, nil))), &set))

	parsed, err := jwt.Parse(token, func(tok *jwt.Token) (any, error) {
		kid, _ := tok.Header["kid"].(string)
		keys := set.Key(kid)
		require.Len(e.t, keys, 1)
		return keys[0].Key, nil
	}, jwt.WithValidMethods([]string{"ES256"}), jwt.WithTimeFunc(e.clock.Now), jwt.WithIssuer("https://hub.keboola.local"), jwt.WithAudience("keboola"))
	require.NoError(e.t, err)
	require.True(e.t, parsed.Valid)

	parts := strings.Split(token, ".")
	require.Len(e.t, parts, 3)
	return verifiedToken{header: jsonSegment(e.t, parts[0]), payload: jsonSegment(e.t, parts[1])}
}

func jsonSegment(t *testing.T, segment string) map[string]any {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	require.NoError(t, err)
	out := map[string]any{}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func TestAppUserToken_OIDCRequest(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
	env.withOIDCWorkspace()
	oidc := env.providers[0]
	client := env.oidcLogin(oidc, &mockoidc.MockUser{Subject: "sub-1", Email: "admin@keboola.com", EmailVerified: new(true), Groups: []string{"admin"}}, oidcOrigin+"/")

	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/some/path").StatusCode)

	upstream := env.lastAppRequest()
	assert.Empty(t, upstream.Header.Values(emailVerifiedHdr))
	assert.Empty(t, upstream.Header.Values("X-Kbc-User-Id"))
	tok := env.verify(upstream.Header.Get(apptoken.HeaderName))
	assert.Equal(t, "kbc-app-user+jwt", tok.header["typ"])
	assert.Equal(t, "keboola", tok.payload["aud"])
	assert.Equal(t, "sub-1", tok.payload["sub"])
	assert.Equal(t, strings.TrimRight(oidc.Issuer(), "/"), tok.payload["idp"])
	assert.Equal(t, "admin@keboola.com", tok.payload["email"])
	assert.Equal(t, []any{"admin"}, tok.payload["groups"])
	assert.Equal(t, map[string]any{"project_id": "123", "app_id": "oidc", "workspace_id": oidcWorkspaceID}, tok.payload["kbc"])
}

func TestAppUserToken_EntraLikeUser(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
	client := env.oidcLogin(env.providers[0], &mockoidc.MockUser{Subject: "sub-2", Email: "someone@tenant.example", Groups: []string{"admin"}}, oidcOrigin+"/")

	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/").StatusCode)

	upstream := env.lastAppRequest()
	assert.Equal(t, "someone@tenant.example", upstream.Header.Get("X-Kbc-User-Email"))
	tok := env.verify(upstream.Header.Get(apptoken.HeaderName))
	assert.Equal(t, "sub-2", tok.payload["sub"])
	assert.NotContains(t, tok.payload, "email")
	assert.Equal(t, map[string]any{"project_id": "123", "app_id": "oidc"}, tok.payload["kbc"])
}

func TestAppUserToken_ForgedHeadersRemoved(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
	client := env.oidcLogin(env.providers[0], &mockoidc.MockUser{Subject: "sub-3", Email: "victim@example.com", Groups: []string{"admin"}}, oidcOrigin+"/")

	resp := env.send(client, oidcOrigin+"/", apptoken.HeaderName, "forged", emailVerifiedHdr, "true")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	upstream := env.lastAppRequest()
	assert.Empty(t, upstream.Header.Values(emailVerifiedHdr))
	require.Len(t, upstream.Header.Values(apptoken.HeaderName), 1)
	tok := env.verify(upstream.Header.Get(apptoken.HeaderName))
	assert.Equal(t, "sub-3", tok.payload["sub"])
	assert.NotContains(t, tok.payload, "email")
}

func TestAppUserToken_NoTokenWithoutOAuth(t *testing.T) {
	t.Parallel()

	forged := []string{apptoken.HeaderName, "forged", "X-Kbc-User-Id", "sub-1", emailVerifiedHdr, "true"}

	t.Run("basic auth", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
		env.syncClock()
		jar, err := cookiejar.New(nil)
		require.NoError(t, err)
		client := *env.client
		client.Jar = jar

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, authOrigin+"/", strings.NewReader("password=abc"))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusMovedPermanently, resp.StatusCode)

		require.Equal(t, http.StatusOK, env.send(&client, authOrigin+"/", forged...).StatusCode)
		assertNoToken(t, env.lastAppRequest())
	})

	t.Run("anonymous rule on an OAuth app", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
		env.syncClock()
		client := env.oidcLogin(env.providers[0], &mockoidc.MockUser{Subject: "sub-4", Email: "a@example.com", EmailVerified: new(true), Groups: []string{"admin"}}, "https://"+prefixHost+"/api")

		require.Equal(t, http.StatusOK, env.send(client, "https://"+prefixHost+"/api").StatusCode)
		env.verify(env.lastAppRequest().Header.Get(apptoken.HeaderName))

		require.Equal(t, http.StatusOK, env.send(client, "https://"+prefixHost+"/public", forged...).StatusCode)
		assertNoToken(t, env.lastAppRequest())
	})

	t.Run("app preview-link session", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
		env.setDevMode("app-auth", authRef, true)
		env.waitForKeys(authHost, authOrigin)
		cookie := env.session(authHost, authOrigin)

		require.Equal(t, http.StatusOK, env.get(authHost, "/", cookie, map[string]string{apptoken.HeaderName: "forged", emailVerifiedHdr: "true"}).StatusCode)
		assertNoToken(t, env.lastAppRequest())
	})

	t.Run("hub host", func(t *testing.T) {
		t.Parallel()
		env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
		before := len(*env.appServer.Requests)
		env.get("hub.keboola.local", "/", nil, map[string]string{apptoken.HeaderName: "forged"})
		assert.Len(t, *env.appServer.Requests, before)
	})
}

func assertNoToken(t *testing.T, upstream *http.Request) {
	t.Helper()
	assert.Empty(t, upstream.Header.Values(apptoken.HeaderName))
	assert.Empty(t, upstream.Header.Values(emailVerifiedHdr))
}

func TestAppUserToken_Rotation(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1", "k2"), "k2"))
	client := env.oidcLogin(env.providers[0], &mockoidc.MockUser{Subject: "sub-5", Email: "a@example.com", Groups: []string{"admin"}}, oidcOrigin+"/")

	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/").StatusCode)
	tok := env.verify(env.lastAppRequest().Header.Get(apptoken.HeaderName))
	assert.Equal(t, "k2", tok.header["kid"])

	var set jose.JSONWebKeySet
	require.NoError(t, json.Unmarshal([]byte(readBody(t, env.get("hub.keboola.local", jwksPath, nil, nil))), &set))
	kids := make([]string, 0, len(set.Keys))
	for _, key := range set.Keys {
		kids = append(kids, key.KeyID)
	}
	assert.Equal(t, []string{"k1", "k2"}, kids)
}
