package apptoken_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

const (
	testAppID       = "app-1"
	testSandbox     = "app-1-sbx"
	testWorkspaceID = "91011"
	testIssuer      = "https://hub.keboola.local"
	testIDP         = "https://idp.example.com"
)

var oidcIDP = apptoken.IdentityProvider{URL: testIDP} //nolint:gochecknoglobals // test fixture

type envOptions struct {
	noKeys      bool
	sessions    bool
	noWorkspace bool
}

type testEnv struct {
	t      *testing.T
	clock  *clockwork.FakeClock
	d      dependencies.ServiceScope
	mocked dependencies.Mocked
	app    api.AppConfig
	ref    k8sapp.WorkloadRef
}

type upstreamRequest struct {
	header  http.Header
	session *sessions.Session
}

func newEnv(t *testing.T, opts envOptions) *testEnv {
	t.Helper()

	cfg := config.New()
	publicURL, err := url.Parse(testIssuer)
	require.NoError(t, err)
	cfg.API.PublicURL = publicURL
	if !opts.noKeys {
		cfg.AppUserToken.SigningKeys = signingKeysJSON(t, "k1", "k2")
		cfg.AppUserToken.ActiveKid = "k2"
	}
	if opts.sessions {
		stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
		t.Cleanup(stream.Close)
		cfg.Sessions.StreamURL = stream.URL
	}

	clock := clockwork.NewFakeClockAt(time.Now())
	d, mocked := dependencies.NewMockedServiceScopeWithK8sObjects(t, t.Context(), cfg, []runtime.Object{appObject()}, commonDeps.WithClock(clock))
	require.True(t, d.AppStateWatcher().WaitForCacheSync(t.Context()))

	e := &testEnv{
		t:      t,
		clock:  clock,
		d:      d,
		mocked: mocked,
		app:    api.AppConfig{ID: testAppID, ProjectID: "123"},
		ref:    k8sapp.WorkloadRef{AppID: testAppID},
	}
	if !opts.noWorkspace {
		e.createSandbox()
	}
	return e
}

// createSandbox goes through the client: the fake client files initial
// objects of kind Sandbox under a guessed resource name the informer never lists.
func (e *testEnv) createSandbox() {
	e.t.Helper()
	_, err := e.mocked.TestFakeK8sClient().Resource(k8sapp.SandboxGVR()).Namespace("keboola").Create(e.t.Context(), sandboxObject(), metav1.CreateOptions{})
	require.NoError(e.t, err)
	require.Eventually(e.t, func() bool {
		info, ok := e.d.AppStateWatcher().GetState(e.t.Context(), e.ref)
		return ok && info.WorkspaceID == testWorkspaceID
	}, 5*time.Second, 10*time.Millisecond)
}

func appObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
		"kind":       "App",
		"metadata":   map[string]any{"name": "app-" + testAppID, "namespace": "keboola"},
		"spec":       map[string]any{"appId": testAppID},
		"status": map[string]any{
			"currentState":      string(k8sapp.AppActualStateRunning),
			"productionSandbox": testSandbox,
		},
	}}
}

func sandboxObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.SandboxVersion,
		"kind":       "Sandbox",
		"metadata":   map[string]any{"name": testSandbox, "namespace": "keboola"},
		"spec":       map[string]any{"appId": testAppID},
		"status": map[string]any{
			"currentState": string(k8sapp.AppActualStateRunning),
			"workspace":    map[string]any{"id": testWorkspaceID, "name": "Workspace"},
		},
	}}
}

func signingKeysJSON(t *testing.T, kids ...string) string {
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

// serve runs one request through the chain the app handler builds.
// A nil idp means the request was not admitted by an OAuth provider.
func (e *testEnv) serve(idp *apptoken.IdentityProvider, headers map[string]string) upstreamRequest {
	e.t.Helper()

	var got upstreamRequest
	upstream := chain.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) error {
		got.header = req.Header.Clone()
		got.session, _ = sessions.FromContext(req.Context())
		return nil
	})
	m := e.d.AppTokenMinter()
	var handler chain.Handler = chain.New(upstream).Prepend(m.Capture(), e.d.SessionsManager().Middleware(e.app, e.ref), m.Inject(e.app, e.ref))
	if idp != nil {
		handler = apptoken.WithIdentityProvider(handler, *idp)
	}

	req := httptest.NewRequestWithContext(e.t.Context(), http.MethodGet, "https://app-1.hub.keboola.local/", nil)
	req.Header.Set("Accept", "text/html")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	require.NoError(e.t, handler.ServeHTTPOrError(httptest.NewRecorder(), req))
	return got
}

type parsedToken struct {
	header  map[string]any
	payload map[string]any
	claims  apptoken.Claims
}

// parse verifies the token against the public keys of the JWKS only.
func (e *testEnv) parse(token string) parsedToken {
	e.t.Helper()

	var set jose.JSONWebKeySet
	require.NoError(e.t, json.Unmarshal(e.d.AppTokenMinter().JWKS(), &set))

	var claims apptoken.Claims
	parsed, err := jwt.ParseWithClaims(token, &claims, func(tok *jwt.Token) (any, error) {
		kid, _ := tok.Header["kid"].(string)
		keys := set.Key(kid)
		require.Len(e.t, keys, 1)
		return keys[0].Key, nil
	}, jwt.WithValidMethods([]string{"ES256"}), jwt.WithTimeFunc(e.clock.Now), jwt.WithIssuer(testIssuer), jwt.WithAudience("keboola"))
	require.NoError(e.t, err)
	require.True(e.t, parsed.Valid)

	parts := strings.Split(token, ".")
	require.Len(e.t, parts, 3)
	return parsedToken{header: decodeSegment(e.t, parts[0]), payload: decodeSegment(e.t, parts[1]), claims: claims}
}

func decodeSegment(t *testing.T, segment string) map[string]any {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	require.NoError(t, err)
	out := map[string]any{}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func identityHeaders() map[string]string {
	return map[string]string{
		"X-Kbc-User-Id":              "sub-1",
		"X-Kbc-User-Email":           "jane@example.com",
		apptoken.EmailVerifiedHeader: "true",
		"X-Kbc-User-Roles":           "a,b",
	}
}

func without(headers map[string]string, names ...string) map[string]string {
	for _, name := range names {
		delete(headers, name)
	}
	return headers
}

func TestInject_FullClaims(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{sessions: true})

	got := e.serve(&oidcIDP, identityHeaders())
	require.NotNil(t, got.session)
	tok := e.parse(got.header.Get(apptoken.HeaderName))

	assert.Equal(t, map[string]any{"alg": "ES256", "kid": "k2", "typ": "kbc-app-user+jwt"}, tok.header)
	assert.Equal(t, "keboola", tok.payload["aud"])
	jti, _ := tok.payload["jti"].(string)
	assert.Regexp(t, `^[0-9a-f]{32}$`, jti)
	iat, _ := tok.payload["iat"].(float64)
	exp, _ := tok.payload["exp"].(float64)
	assert.InDelta(t, (5 * time.Minute).Seconds(), exp-iat, 0)
	assert.InDelta(t, float64(e.clock.Now().Unix()), iat, 0)

	delete(tok.payload, "jti")
	delete(tok.payload, "iat")
	delete(tok.payload, "exp")
	assert.Equal(t, map[string]any{
		"iss":    testIssuer,
		"aud":    "keboola",
		"sub":    "sub-1",
		"idp":    testIDP,
		"email":  "jane@example.com",
		"groups": []any{"a", "b"},
		"kbc":    map[string]any{"project_id": "123", "app_id": testAppID, "workspace_id": testWorkspaceID},
		"sid":    got.session.ID,
	}, tok.payload)
}

func TestInject_EmailOmittedWhenNotVerified(t *testing.T) {
	t.Parallel()

	for name, verified := range map[string]string{"missing": "", "false": "false"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, envOptions{})
			headers := identityHeaders()
			headers[apptoken.EmailVerifiedHeader] = verified
			if verified == "" {
				delete(headers, apptoken.EmailVerifiedHeader)
			}
			tok := e.parse(e.serve(&oidcIDP, headers).header.Get(apptoken.HeaderName))
			assert.NotContains(t, tok.payload, "email")
			assert.Equal(t, "sub-1", tok.claims.Subject)
		})
	}
}

func TestInject_EmailNeedsTheClaimForEveryProvider(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	idp := apptoken.IdentityProvider{URL: "https://github.com"}
	tok := e.parse(e.serve(&idp, without(identityHeaders(), apptoken.EmailVerifiedHeader)).header.Get(apptoken.HeaderName))
	assert.NotContains(t, tok.payload, "email")
	assert.Equal(t, "https://github.com", tok.payload["idp"])
}

func TestInject_GroupsUnknownOmitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	tok := e.parse(e.serve(&oidcIDP, without(identityHeaders(), "X-Kbc-User-Roles")).header.Get(apptoken.HeaderName))
	assert.NotContains(t, tok.payload, "groups")
}

func TestInject_GroupsTooLargeOmitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	headers := identityHeaders()
	headers["X-Kbc-User-Roles"] = strings.TrimSuffix(strings.Repeat("group-0123456789,", 300), ",")
	require.Greater(t, len(headers["X-Kbc-User-Roles"]), 4096)
	tok := e.parse(e.serve(&oidcIDP, headers).header.Get(apptoken.HeaderName))
	assert.NotContains(t, tok.payload, "groups")
	assert.Equal(t, "sub-1", tok.claims.Subject)
}

func TestInject_GroupsSplitAcrossHeaderValues(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	headers := identityHeaders()
	headers["X-Kbc-User-Roles"] = "a,,b,"
	tok := e.parse(e.serve(&oidcIDP, headers).header.Get(apptoken.HeaderName))
	assert.Equal(t, []any{"a", "b"}, tok.payload["groups"])
}

func TestInject_WorkspaceUnknownOmitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{noWorkspace: true})

	tok := e.parse(e.serve(&oidcIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	assert.Equal(t, map[string]any{"project_id": "123", "app_id": testAppID}, tok.payload["kbc"])
}

func TestInject_NoSessionNoSID(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve(&oidcIDP, identityHeaders())
	assert.Nil(t, got.session)
	tok := e.parse(got.header.Get(apptoken.HeaderName))
	assert.NotContains(t, tok.payload, "sid")
}

func TestInject_SkipsWithoutIdentityProvider(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve(nil, identityHeaders())
	assert.Empty(t, got.header.Values(apptoken.HeaderName))
}

func TestInject_SkipsWithoutUserID(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve(&oidcIDP, without(identityHeaders(), "X-Kbc-User-Id"))
	assert.Empty(t, got.header.Values(apptoken.HeaderName))
}

func TestInject_SkipsWhenDisabled(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{noKeys: true})

	got := e.serve(&oidcIDP, identityHeaders())
	assert.Empty(t, got.header.Values(apptoken.HeaderName))
}

func TestInject_JTIUnique(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	first := e.parse(e.serve(&oidcIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	second := e.parse(e.serve(&oidcIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	assert.NotEqual(t, first.claims.ID, second.claims.ID)
}

func TestCapture_StripsEmailVerifiedHeader(t *testing.T) {
	t.Parallel()

	for name, idp := range map[string]*apptoken.IdentityProvider{"oauth": &oidcIDP, "no oauth": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, envOptions{})
			got := e.serve(idp, identityHeaders())
			assert.Empty(t, got.header.Values(apptoken.EmailVerifiedHeader))
		})
	}
}

func TestInject_ReplacesInboundToken(t *testing.T) {
	t.Parallel()

	t.Run("minted", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		headers := identityHeaders()
		headers[apptoken.HeaderName] = "forged"
		got := e.serve(&oidcIDP, headers)
		require.Len(t, got.header.Values(apptoken.HeaderName), 1)
		e.parse(got.header.Get(apptoken.HeaderName))
	})

	t.Run("skipped", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		got := e.serve(nil, map[string]string{apptoken.HeaderName: "forged"})
		assert.Empty(t, got.header.Values(apptoken.HeaderName))
	})
}
