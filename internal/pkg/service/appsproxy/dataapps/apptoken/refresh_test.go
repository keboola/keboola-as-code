package apptoken_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
)

const wsTimeout = 6 * time.Hour

func wsHeaders(headers map[string]string) map[string]string {
	headers["Connection"] = "Upgrade"
	headers["Upgrade"] = "websocket"
	return headers
}

// parseRefresh verifies the refresh token against the configured refresh keys.
func (e *testEnv) parseRefresh(token string) parsedToken {
	e.t.Helper()
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(tok *jwt.Token) (any, error) {
		kid, _ := tok.Header["kid"].(string)
		secret, ok := e.refreshKeys[kid]
		require.True(e.t, ok, kid)
		return secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithTimeFunc(e.clock.Now), jwt.WithIssuer(testIssuer), jwt.WithAudience("apps-proxy"))
	require.NoError(e.t, err)
	require.True(e.t, parsed.Valid)

	parts := strings.Split(token, ".")
	require.Len(e.t, parts, 3)
	return parsedToken{header: decodeSegment(e.t, parts[0]), payload: decodeSegment(e.t, parts[1])}
}

// handshake runs a websocket handshake and returns the upstream request.
func (e *testEnv) handshake(ref k8sapp.WorkloadRef) upstreamRequest {
	e.t.Helper()
	h := e.newHandler(testIDP)
	if ref != e.ref {
		h = e.newHandlerFor(ref)
	}
	got := h.serve(wsHeaders(identityHeaders()))
	require.NotEmpty(e.t, got.header.Get(apptoken.RefreshHeaderName))
	return got
}

// resign signs a modified copy of a refresh token's claims.
func (e *testEnv) resign(token string, method jwt.SigningMethod, key any, header map[string]any, mutate func(claims jwt.MapClaims)) string {
	e.t.Helper()
	parts := strings.Split(token, ".")
	require.Len(e.t, parts, 3)
	claims := jwt.MapClaims(decodeSegment(e.t, parts[1]))
	if mutate != nil {
		mutate(claims)
	}
	tok := jwt.NewWithClaims(method, claims)
	for k, v := range decodeSegment(e.t, parts[0]) {
		if k != "alg" {
			tok.Header[k] = v
		}
	}
	for k, v := range header {
		tok.Header[k] = v
	}
	out, err := tok.SignedString(key)
	require.NoError(e.t, err)
	return out
}

func (e *testEnv) refresh(app api.AppConfig, ref k8sapp.WorkloadRef, token string) (string, time.Time, error) {
	e.t.Helper()
	return e.d.AppTokenMinter().Refresh(e.t.Context(), app, ref, token)
}

func TestInjectRefresh_WebsocketHandshake(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{sessions: true})

	got := e.serve(testIDP, wsHeaders(identityHeaders()))
	require.NotNil(t, got.session)
	e.parse(got.header.Get(apptoken.HeaderName))
	assert.Equal(t, "https://"+testAppHost, got.header.Get(apptoken.PublicURLHeaderName))

	tok := e.parseRefresh(got.header.Get(apptoken.RefreshHeaderName))
	assert.Equal(t, map[string]any{"alg": "HS256", "kid": "k2", "typ": "kbc-app-user-refresh+jwt"}, tok.header)
	jti, _ := tok.payload["jti"].(string)
	assert.Regexp(t, `^[0-9a-f]{32}$`, jti)
	iat, _ := tok.payload["iat"].(float64)
	exp, _ := tok.payload["exp"].(float64)
	assert.InDelta(t, float64(e.clock.Now().Unix()), iat, 0)
	// No session deadline in the context: the one-hour cap of a websocket with an app user token applies.
	assert.InDelta(t, time.Hour.Seconds(), exp-iat, 0)

	delete(tok.payload, "jti")
	delete(tok.payload, "iat")
	delete(tok.payload, "exp")
	assert.Equal(t, map[string]any{
		"iss":    testIssuer,
		"aud":    "apps-proxy",
		"sub":    "sub-1",
		"idp":    testIDP,
		"email":  "jane@example.com",
		"groups": []any{"a", "b"},
		"kbc":    map[string]any{"project_id": "123", "app_id": testAppID, "auth_provider_id": testProviderID},
		"sid":    got.session.ID,
	}, tok.payload)
}

func TestInjectRefresh_ExpiresAtSessionDeadline(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	deadline := e.clock.Now().Add(time.Hour).Truncate(time.Second)
	ctx := upstream.WithSessionDeadline(t.Context(), deadline, upstream.CloseReasonRefreshDue)
	got := e.newHandler(testIDP).serveCtx(ctx, wsHeaders(identityHeaders()))

	tok := e.parseRefresh(got.header.Get(apptoken.RefreshHeaderName))
	assert.InDelta(t, float64(deadline.Unix()), tok.payload["exp"], 0)
}

func TestInject_CapsTheWebsocketDeadline(t *testing.T) {
	t.Parallel()
	opened := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

	t.Run("with an app user token", func(t *testing.T) {
		t.Parallel()
		got := newEnv(t, envOptions{}).serve(testIDP, wsHeaders(identityHeaders()))
		require.NotEmpty(t, got.header.Get(apptoken.HeaderName))
		at, reason := upstream.WebsocketDeadline(got.ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(time.Hour), at)
		assert.Equal(t, upstream.CloseReasonAppUserTokenCap, reason)
	})

	t.Run("without an app user token", func(t *testing.T) {
		t.Parallel()
		got := newEnv(t, envOptions{noKeys: true}).serve(testIDP, wsHeaders(identityHeaders()))
		require.Empty(t, got.header.Get(apptoken.HeaderName))
		at, _ := upstream.WebsocketDeadline(got.ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(wsTimeout), at)
	})
}

func TestInjectRefresh_Sandbox(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	ref := k8sapp.WorkloadRef{AppID: testAppID, SandboxName: testSandbox}

	got := e.handshake(ref)
	assert.Equal(t, "https://"+testSandbox+".hub.keboola.local", got.header.Get(apptoken.PublicURLHeaderName))
	tok := e.parseRefresh(got.header.Get(apptoken.RefreshHeaderName))
	assert.Equal(t, map[string]any{"project_id": "123", "app_id": testAppID, "sandbox": testSandbox, "auth_provider_id": testProviderID}, tok.payload["kbc"])
}

func TestInjectRefresh_OnlyOnWebsocketWithToken(t *testing.T) {
	t.Parallel()

	assertNone := func(t *testing.T, got upstreamRequest) {
		t.Helper()
		assert.Empty(t, got.header.Values(apptoken.RefreshHeaderName))
		assert.Empty(t, got.header.Values(apptoken.PublicURLHeaderName))
	}

	t.Run("plain request", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		got := e.serve(testIDP, identityHeaders())
		require.NotEmpty(t, got.header.Get(apptoken.HeaderName))
		assertNone(t, got)
	})

	t.Run("no identity", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		assertNone(t, e.serve("", wsHeaders(identityHeaders())))
	})

	t.Run("signing keys off", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{noKeys: true})
		assertNone(t, e.serve(testIDP, wsHeaders(identityHeaders())))
	})

	t.Run("inbound copies on a plain request", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		headers := identityHeaders()
		headers[apptoken.RefreshHeaderName] = "forged"
		headers[apptoken.PublicURLHeaderName] = "https://evil.example.com"
		assertNone(t, e.serve(testIDP, headers))
	})

	t.Run("inbound copies on a handshake without identity", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		headers := wsHeaders(map[string]string{})
		headers[apptoken.RefreshHeaderName] = "forged"
		headers[apptoken.PublicURLHeaderName] = "https://evil.example.com"
		assertNone(t, e.serve("", headers))
	})

	t.Run("inbound copies replaced on a handshake", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		headers := wsHeaders(identityHeaders())
		headers[apptoken.RefreshHeaderName] = "forged"
		headers[apptoken.PublicURLHeaderName] = "https://evil.example.com"
		got := e.serve(testIDP, headers)
		require.Len(t, got.header.Values(apptoken.RefreshHeaderName), 1)
		e.parseRefresh(got.header.Get(apptoken.RefreshHeaderName))
		assert.Equal(t, []string{"https://" + testAppHost}, got.header.Values(apptoken.PublicURLHeaderName))
	})
}

func TestRefreshToken_NotVerifiableWithJWKS(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	refresh := e.handshake(e.ref).header.Get(apptoken.RefreshHeaderName)

	var set jose.JSONWebKeySet
	require.NoError(t, json.Unmarshal(e.d.AppTokenMinter().JWKS(), &set))
	_, err := jwt.Parse(refresh, func(tok *jwt.Token) (any, error) {
		kid, _ := tok.Header["kid"].(string)
		keys := set.Key(kid)
		require.Len(t, keys, 1, "the refresh kid matches a JWKS kid, so the test fails on the key type, not on the kid")
		return keys[0].Key, nil
	}, jwt.WithTimeFunc(e.clock.Now))
	require.Error(t, err)
}

func TestRefresh_MintsLikeTheHandshake(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{sessions: true})
	got := e.handshake(e.ref)
	injected := e.parse(got.header.Get(apptoken.HeaderName))

	e.clock.Advance(4 * time.Minute)
	token, expiresAt, err := e.refresh(e.app, e.ref, got.header.Get(apptoken.RefreshHeaderName))
	require.NoError(t, err)
	refreshed := e.parse(token)

	assert.Equal(t, injected.header, refreshed.header)
	assert.NotEqual(t, injected.claims.ID, refreshed.claims.ID)
	assert.Equal(t, e.clock.Now().Unix(), refreshed.claims.IssuedAt.Unix())
	assert.Equal(t, e.clock.Now().Add(5*time.Minute).Unix(), refreshed.claims.ExpiresAt.Unix())
	assert.Equal(t, refreshed.claims.ExpiresAt.Time, expiresAt)
	for _, claim := range []string{"jti", "iat", "exp"} {
		delete(injected.payload, claim)
		delete(refreshed.payload, claim)
	}
	assert.Equal(t, injected.payload, refreshed.payload)
	assert.Equal(t, got.session.ID, refreshed.payload["sid"])
}

func TestRefresh_NeverOutlivesTheRefreshToken(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	deadline := e.clock.Now().Add(2 * time.Minute).Truncate(time.Second)
	ctx := upstream.WithSessionDeadline(t.Context(), deadline, upstream.CloseReasonRefreshDue)
	got := e.newHandler(testIDP).serveCtx(ctx, wsHeaders(identityHeaders()))

	token, expiresAt, err := e.refresh(e.app, e.ref, got.header.Get(apptoken.RefreshHeaderName))
	require.NoError(t, err)
	assert.Equal(t, deadline.Unix(), e.parse(token).claims.ExpiresAt.Unix())
	assert.Equal(t, deadline.Unix(), expiresAt.Unix())
}

func TestRefresh_ReadsTheWorkspaceAgain(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	refresh := e.handshake(e.ref).header.Get(apptoken.RefreshHeaderName)

	e.promote("app-1-sbx-2", "91012")
	token, _, err := e.refresh(e.app, e.ref, refresh)
	require.NoError(t, err)
	assert.Equal(t, "91012", e.parse(token).claims.Kbc.WorkspaceID)
}

func TestRefresh_Sandbox(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	ref := k8sapp.WorkloadRef{AppID: testAppID, SandboxName: testSandbox}
	refresh := e.handshake(ref).header.Get(apptoken.RefreshHeaderName)

	token, _, err := e.refresh(e.app, ref, refresh)
	require.NoError(t, err)
	assert.Equal(t, testAppID, e.parse(token).claims.Kbc.AppID)
}

func TestRefresh_Enabled(t *testing.T) {
	t.Parallel()
	assert.True(t, newEnv(t, envOptions{noWorkspace: true}).d.AppTokenMinter().RefreshEnabled())
	assert.False(t, newEnv(t, envOptions{noWorkspace: true, noKeys: true}).d.AppTokenMinter().RefreshEnabled())
}

func TestRefresh_Refused(t *testing.T) {
	t.Parallel()

	sandboxRef := k8sapp.WorkloadRef{AppID: testAppID, SandboxName: testSandbox}
	otherApp := testApp()
	otherApp.ID = "app-2"
	otherProject := testApp()
	otherProject.ProjectID = "456"
	noProvider := testApp()
	noProvider.AuthProviders = provider.Providers{}
	noProvider.AuthRules = []api.Rule{{Type: api.RulePathPrefix, Value: "/", AuthRequired: new(bool)}}
	anonymous := testApp()
	anonymous.AuthRules = []api.Rule{{Type: api.RulePathPrefix, Value: "/", AuthRequired: new(bool)}}
	otherIssuer := testApp()
	otherIssuer.AuthProviders = provider.Providers{
		provider.OIDC{Base: provider.Base{Info: provider.Info{ID: testProviderID, Type: provider.TypeOIDC}}, IssuerURL: "https://other-idp.example.com"},
	}
	basicProvider := testApp()
	basicProvider.AuthProviders = provider.Providers{provider.Basic{Base: provider.Base{Info: provider.Info{ID: testProviderID, Type: provider.TypeBasic}}}}

	cases := []struct {
		name  string
		token func(e *testEnv, refresh, appToken string) string
		app   *api.AppConfig
		ref   *k8sapp.WorkloadRef
		setup func(e *testEnv)
	}{
		{name: "empty", token: func(_ *testEnv, _, _ string) string { return "" }},
		{name: "garbage", token: func(_ *testEnv, _, _ string) string { return "not.a.jwt" }},
		{name: "expired", setup: func(e *testEnv) { e.clock.Advance(wsTimeout + time.Second) }},
		{name: "other app", app: &otherApp, ref: &k8sapp.WorkloadRef{AppID: "app-2"}},
		{name: "other project", app: &otherProject},
		{name: "app token on the sandbox", ref: &sandboxRef},
		{name: "provider removed", app: &noProvider},
		{name: "app no longer requires login", app: &anonymous},
		{name: "provider issuer changed", app: &otherIssuer},
		{name: "provider is not oauth", app: &basicProvider},
		{name: "app user token", token: func(_ *testEnv, _, appToken string) string { return appToken }},
		{name: "wrong typ", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], map[string]any{"typ": "kbc-app-user+jwt"}, nil)
		}},
		{name: "no typ", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], map[string]any{"typ": nil}, nil)
		}},
		{name: "wrong aud", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], nil, func(c jwt.MapClaims) { c["aud"] = "keboola" })
		}},
		{name: "app user typ and aud", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], map[string]any{"typ": "kbc-app-user+jwt"}, func(c jwt.MapClaims) { c["aud"] = "keboola" })
		}},
		{name: "wrong iss", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], nil, func(c jwt.MapClaims) { c["iss"] = "https://other.example.com" })
		}},
		{name: "no exp", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], nil, func(c jwt.MapClaims) { delete(c, "exp") })
		}},
		{name: "unknown kid", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k2"], map[string]any{"kid": "unknown"}, nil)
		}},
		{name: "wrong secret", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, []byte(strings.Repeat("x", 32)), nil, nil)
		}},
		{name: "secret of another kid", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k1"], nil, nil)
		}},
		{name: "alg none", token: func(e *testEnv, refresh, _ string) string {
			return e.resign(refresh, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, nil, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, envOptions{})
			got := e.handshake(e.ref)
			refresh := got.header.Get(apptoken.RefreshHeaderName)
			if tc.token != nil {
				refresh = tc.token(e, refresh, got.header.Get(apptoken.HeaderName))
			}
			if tc.setup != nil {
				tc.setup(e)
			}
			app, ref := e.app, e.ref
			if tc.app != nil {
				app = *tc.app
			}
			if tc.ref != nil {
				ref = *tc.ref
			}
			token, _, err := e.refresh(app, ref, refresh)
			require.Error(t, err)
			assert.Empty(t, token)
		})
	}
}

func TestRefresh_AcceptsAnInactiveSigningKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	refresh := e.handshake(e.ref).header.Get(apptoken.RefreshHeaderName)
	refresh = e.resign(refresh, jwt.SigningMethodHS256, e.refreshKeys["k1"], map[string]any{"kid": "k1"}, nil)

	token, _, err := e.refresh(e.app, e.ref, refresh)
	require.NoError(t, err)
	assert.Equal(t, "k2", e.parse(token).header["kid"])
}

func TestRefresh_SandboxTokenRefusedOnTheApp(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	refresh := e.handshake(k8sapp.WorkloadRef{AppID: testAppID, SandboxName: testSandbox}).header.Get(apptoken.RefreshHeaderName)

	_, _, err := e.refresh(e.app, e.ref, refresh)
	require.Error(t, err)
}

func TestRefresh_RefusedWhenSigningKeysOff(t *testing.T) {
	t.Parallel()
	on := newEnv(t, envOptions{})
	refresh := on.handshake(on.ref).header.Get(apptoken.RefreshHeaderName)

	refresh = on.resign(refresh, jwt.SigningMethodHS256, on.refreshKeys["k2"], nil, func(c jwt.MapClaims) {
		c["exp"] = float64(on.clock.Now().Add(time.Hour).Unix())
	})
	on.parseRefresh(refresh)

	off := newEnv(t, envOptions{noKeys: true})

	_, _, err := off.refresh(off.app, off.ref, refresh)
	require.Error(t, err)
}
