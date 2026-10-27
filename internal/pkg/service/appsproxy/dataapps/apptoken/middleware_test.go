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
	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"

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

type envOptions struct {
	noKeys      bool
	sessions    bool
	noWorkspace bool
	ttl         time.Duration
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
	if opts.ttl != 0 {
		cfg.AppUserToken.TTL = opts.ttl
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
	e.createSandboxObject(testSandbox, testWorkspaceID)
	e.waitForWorkspace(testWorkspaceID)
}

func (e *testEnv) createSandboxObject(name, workspaceID string) {
	e.t.Helper()
	_, err := e.mocked.TestFakeK8sClient().Resource(k8sapp.SandboxGVR()).Namespace("keboola").Create(e.t.Context(), sandboxObject(name, workspaceID), metav1.CreateOptions{})
	require.NoError(e.t, err)
}

// promote points the App's status.productionSandbox at another Sandbox.
func (e *testEnv) promote(name, workspaceID string) {
	e.t.Helper()
	e.createSandboxObject(name, workspaceID)
	patch := []byte(`{"status":{"productionSandbox":"` + name + `"}}`)
	_, err := e.mocked.TestFakeK8sClient().Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(e.t.Context(), "app-"+testAppID, k8stypes.MergePatchType, patch, metav1.PatchOptions{})
	require.NoError(e.t, err)
	e.waitForWorkspace(workspaceID)
}

func (e *testEnv) waitForWorkspace(workspaceID string) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		info, ok := e.d.AppStateWatcher().GetState(e.t.Context(), e.ref)
		return ok && info.WorkspaceID == workspaceID
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

func sandboxObject(name, workspaceID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.SandboxVersion,
		"kind":       "Sandbox",
		"metadata":   map[string]any{"name": name, "namespace": "keboola"},
		"spec":       map[string]any{"appId": testAppID},
		"status": map[string]any{
			"currentState": string(k8sapp.AppActualStateRunning),
			"workspace":    map[string]any{"id": workspaceID, "name": "Workspace"},
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
// An empty idp means the request was not admitted by an OAuth provider.
func (e *testEnv) serve(idp string, headers map[string]string) upstreamRequest {
	e.t.Helper()
	return e.newHandler(idp).serve(headers)
}

// testHandler is one chain, built once like the app handler builds it, so a
// test can send several requests through the same instance.
type testHandler struct {
	e       *testEnv
	handler chain.Handler
	got     *upstreamRequest
}

func (e *testEnv) newHandler(idp string) *testHandler {
	got := &upstreamRequest{}
	upstream := chain.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) error {
		got.header = req.Header.Clone()
		got.session, _ = sessions.FromContext(req.Context())
		return nil
	})
	m := e.d.AppTokenMinter()
	var handler chain.Handler = chain.New(upstream).Prepend(e.d.SessionsManager().Middleware(e.app, e.ref), m.Inject(e.app, e.ref))
	if idp != "" {
		handler = apptoken.WithIdentity(handler, idp)
	}
	return &testHandler{e: e, handler: handler, got: got}
}

func (h *testHandler) serve(headers map[string]string) upstreamRequest {
	h.e.t.Helper()
	*h.got = upstreamRequest{}
	req := httptest.NewRequestWithContext(h.e.t.Context(), http.MethodGet, "https://app-1.hub.keboola.local/", nil)
	req.Header.Set("Accept", "text/html")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	require.NoError(h.e.t, h.handler.ServeHTTPOrError(httptest.NewRecorder(), req))
	return *h.got
}

type parsedToken struct {
	header  map[string]any
	payload map[string]any
	claims  sdk.Claims
}

// parse verifies the token against the public keys of the JWKS only.
func (e *testEnv) parse(token string) parsedToken {
	e.t.Helper()

	var set jose.JSONWebKeySet
	require.NoError(e.t, json.Unmarshal(e.d.AppTokenMinter().JWKS(), &set))

	var claims sdk.Claims
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

	got := e.serve(testIDP, identityHeaders())
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

func TestInject_TTLFromConfig(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{ttl: time.Minute})

	tok := e.parse(e.serve(testIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	assert.Equal(t, time.Minute, tok.claims.ExpiresAt.Sub(tok.claims.IssuedAt.Time))
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
			tok := e.parse(e.serve(testIDP, headers).header.Get(apptoken.HeaderName))
			assert.NotContains(t, tok.payload, "email")
			assert.Equal(t, "sub-1", tok.claims.Subject)
		})
	}
}

func TestInject_GroupsUnknownOmitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	tok := e.parse(e.serve(testIDP, without(identityHeaders(), "X-Kbc-User-Roles")).header.Get(apptoken.HeaderName))
	assert.NotContains(t, tok.payload, "groups")
}

func TestInject_GroupsSizeBoundary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		roles  string
		groups []any
	}{
		{name: "4096 bytes kept", roles: strings.Repeat("g", 4094) + ",h", groups: []any{strings.Repeat("g", 4094), "h"}},
		{name: "4097 bytes omitted", roles: strings.Repeat("g", 4095) + ",h"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, envOptions{})
			headers := identityHeaders()
			headers["X-Kbc-User-Roles"] = tc.roles
			tok := e.parse(e.serve(testIDP, headers).header.Get(apptoken.HeaderName))
			if tc.groups == nil {
				assert.NotContains(t, tok.payload, "groups")
				return
			}
			assert.Equal(t, tc.groups, tok.payload["groups"])
		})
	}
}

func TestInject_GroupsEmptyItemsDropped(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	headers := identityHeaders()
	headers["X-Kbc-User-Roles"] = "a,,b,"
	tok := e.parse(e.serve(testIDP, headers).header.Get(apptoken.HeaderName))
	assert.Equal(t, []any{"a", "b"}, tok.payload["groups"])
}

func TestInject_WorkspaceUnknownOmitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{noWorkspace: true})

	tok := e.parse(e.serve(testIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	assert.Equal(t, map[string]any{"project_id": "123", "app_id": testAppID}, tok.payload["kbc"])
}

func TestInject_WorkspaceFollowsPromotion(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	handler := e.newHandler(testIDP)

	before := e.parse(handler.serve(identityHeaders()).header.Get(apptoken.HeaderName))
	assert.Equal(t, testWorkspaceID, before.claims.Kbc.WorkspaceID)

	e.promote("app-1-sbx-next", "91012")

	after := e.parse(handler.serve(identityHeaders()).header.Get(apptoken.HeaderName))
	assert.Equal(t, "91012", after.claims.Kbc.WorkspaceID)
}

func TestInject_NoSessionNoSID(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve(testIDP, identityHeaders())
	assert.Nil(t, got.session)
	tok := e.parse(got.header.Get(apptoken.HeaderName))
	assert.NotContains(t, tok.payload, "sid")
}

func TestInject_SkipsWithoutIdentityProvider(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve("", identityHeaders())
	assert.Empty(t, got.header.Values(apptoken.HeaderName))
}

func TestInject_SkipsWithoutUserID(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve(testIDP, without(identityHeaders(), "X-Kbc-User-Id"))
	assert.Empty(t, got.header.Values(apptoken.HeaderName))
}

func TestInject_SkipsWhenDisabled(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{noKeys: true})

	got := e.serve(testIDP, identityHeaders())
	assert.Empty(t, got.header.Values(apptoken.HeaderName))
}

func TestInject_JTIUnique(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	first := e.parse(e.serve(testIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	second := e.parse(e.serve(testIDP, identityHeaders()).header.Get(apptoken.HeaderName))
	assert.NotEqual(t, first.claims.ID, second.claims.ID)
}

func TestWithIdentity_StripsEmailVerifiedHeader(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	got := e.serve(testIDP, identityHeaders())
	assert.Empty(t, got.header.Values(apptoken.EmailVerifiedHeader))
}

func TestInject_ReplacesInboundToken(t *testing.T) {
	t.Parallel()

	t.Run("minted", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		headers := identityHeaders()
		headers[apptoken.HeaderName] = "forged"
		got := e.serve(testIDP, headers)
		require.Len(t, got.header.Values(apptoken.HeaderName), 1)
		e.parse(got.header.Get(apptoken.HeaderName))
	})

	t.Run("skipped", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		got := e.serve("", map[string]string{apptoken.HeaderName: "forged"})
		assert.Empty(t, got.header.Values(apptoken.HeaderName))
	})

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{noKeys: true})
		headers := identityHeaders()
		headers[apptoken.HeaderName] = "forged"
		got := e.serve(testIDP, headers)
		assert.Empty(t, got.header.Values(apptoken.HeaderName))
	})
}
