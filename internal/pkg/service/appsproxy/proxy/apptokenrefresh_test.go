package proxy_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
)

// websocketHandshake logs in to the "oidc" app, opens a websocket and returns the handshake the app received.
func (e *previewEnv) websocketHandshake() *http.Request {
	e.t.Helper()
	client := e.oidcLogin(e.providers[0], freshnessUser(), oidcOrigin+"/")
	return e.dialWebsocket(client, nil)
}

// dialWebsocket reads the one message the app's /ws endpoint sends and closes the socket as that endpoint expects.
func (e *previewEnv) dialWebsocket(client *http.Client, header http.Header) *http.Request {
	e.t.Helper()
	c, resp, err := websocket.Dial(e.t.Context(), "wss://"+oidcHost+"/ws", &websocket.DialOptions{HTTPClient: client, HTTPHeader: header})
	require.NoError(e.t, err)
	require.Equal(e.t, http.StatusSwitchingProtocols, resp.StatusCode)
	var v any
	require.NoError(e.t, wsjson.Read(e.t.Context(), c, &v))
	require.NoError(e.t, c.Close(websocket.StatusNormalClosure, ""))
	handshake := e.lastAppRequest()
	require.Equal(e.t, "/ws", handshake.URL.Path)
	return handshake
}

func (e *previewEnv) post(target string, headers ...string) *http.Response {
	e.t.Helper()
	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodPost, target, nil)
	require.NoError(e.t, err)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := e.client.Do(req)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func startRefreshEnv(t *testing.T) *previewEnv {
	t.Helper()
	env := startPreviewEnv(t, withAppUserTokenKeys(appTokenSigningKeys(t, "k1"), "k1"))
	env.withOIDCWorkspace()
	env.syncClock()
	return env
}

func assertRefused(t *testing.T, resp *http.Response) {
	t.Helper()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.Equal(t, "Bearer", resp.Header.Get("WWW-Authenticate"))
	assert.NotContains(t, readBody(t, resp), "token")
}

func TestAppUserTokenRefresh_WebsocketHandshake(t *testing.T) {
	t.Parallel()
	env := startRefreshEnv(t)
	handshake := env.websocketHandshake()

	assert.Equal(t, oidcOrigin, handshake.Header.Get(apptoken.PublicURLHeaderName))
	refresh := handshake.Header.Get(apptoken.RefreshHeaderName)
	require.NotEmpty(t, refresh)
	parts := strings.Split(refresh, ".")
	require.Len(t, parts, 3)
	payload := jsonSegment(t, parts[1])
	iat, _ := payload["iat"].(float64)
	exp, _ := payload["exp"].(float64)
	// The login session has a refresh token, so the websocket closes when the session is due for refresh, not at the 6 h timeout.
	assert.InDelta(t, time.Hour.Seconds(), exp-iat, 30)
}

func TestAppUserTokenRefresh_NotOnPlainRequest(t *testing.T) {
	t.Parallel()
	env := startRefreshEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

	require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/", apptoken.RefreshHeaderName, "forged", apptoken.PublicURLHeaderName, "https://evil.example.com").StatusCode)
	upstream := env.lastAppRequest()
	require.NotEmpty(t, upstream.Header.Get(apptoken.HeaderName))
	assert.Empty(t, upstream.Header.Values(apptoken.RefreshHeaderName))
	assert.Empty(t, upstream.Header.Values(apptoken.PublicURLHeaderName))
}

func TestAppUserTokenRefresh_ForgedHeadersOnHandshakeReplaced(t *testing.T) {
	t.Parallel()
	env := startRefreshEnv(t)
	client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")

	header := http.Header{}
	header.Set(apptoken.RefreshHeaderName, "forged")
	header.Set(apptoken.PublicURLHeaderName, "https://evil.example.com")
	upstream := env.dialWebsocket(client, header)
	assert.Equal(t, []string{oidcOrigin}, upstream.Header.Values(apptoken.PublicURLHeaderName))
	require.Len(t, upstream.Header.Values(apptoken.RefreshHeaderName), 1)
	assert.NotEqual(t, "forged", upstream.Header.Get(apptoken.RefreshHeaderName))
}

func TestAppUserTokenRefresh_Endpoint(t *testing.T) {
	t.Parallel()
	env := startRefreshEnv(t)
	handshake := env.websocketHandshake()
	refresh := handshake.Header.Get(apptoken.RefreshHeaderName)
	injected := env.verify(handshake.Header.Get(apptoken.HeaderName))
	appRequests := len(*env.appServer.Requests)

	resp := env.post(handshake.Header.Get(apptoken.PublicURLHeaderName)+apptoken.RefreshPath, "Authorization", "Bearer "+refresh)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.Empty(t, resp.Header.Values("Set-Cookie"))
	assert.Empty(t, resp.Header.Values("Access-Control-Allow-Origin"))

	var body struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expiresAt"`
	}
	require.NoError(t, json.Unmarshal([]byte(readBody(t, resp)), &body))
	tok := env.verify(body.Token)
	expiresAt, err := time.Parse(time.RFC3339, body.ExpiresAt)
	require.NoError(t, err)
	exp, _ := tok.payload["exp"].(float64)
	assert.InDelta(t, exp, float64(expiresAt.Unix()), 0)
	assert.NotEqual(t, injected.payload["jti"], tok.payload["jti"])
	for _, claim := range []string{"jti", "iat", "exp"} {
		delete(injected.payload, claim)
		delete(tok.payload, claim)
	}
	assert.Equal(t, injected.payload, tok.payload)
	assert.Equal(t, map[string]any{"project_id": "123", "app_id": "oidc", "workspace_id": oidcWorkspaceID}, tok.payload["kbc"])

	// The internal path never reaches the app, so it is neither session nor idle-suspend activity.
	assert.Len(t, *env.appServer.Requests, appRequests)

	secrets := []string{refresh, body.Token}
	logs := env.mocked.DebugLogger().AllMessages()
	spans := fmt.Sprintf("%+v", env.mocked.TestTelemetry().Spans(t))
	for _, secret := range secrets {
		assert.NotContains(t, logs, secret)
		assert.NotContains(t, spans, secret)
	}
}

func TestAppUserTokenRefresh_EndpointRefuses(t *testing.T) {
	t.Parallel()
	env := startRefreshEnv(t)
	refresh := env.websocketHandshake().Header.Get(apptoken.RefreshHeaderName)
	endpoint := oidcOrigin + apptoken.RefreshPath

	t.Run("no credential", func(t *testing.T) {
		t.Parallel()
		assertRefused(t, env.post(endpoint))
	})
	t.Run("not a bearer credential", func(t *testing.T) {
		t.Parallel()
		assertRefused(t, env.post(endpoint, "Authorization", "Basic "+refresh))
	})
	t.Run("invalid token", func(t *testing.T) {
		t.Parallel()
		assertRefused(t, env.post(endpoint, "Authorization", "Bearer not.a.jwt"))
	})
	t.Run("refresh token in an internal header", func(t *testing.T) {
		t.Parallel()
		assertRefused(t, env.post(endpoint, apptoken.RefreshHeaderName, refresh))
	})
	t.Run("app user token", func(t *testing.T) {
		t.Parallel()
		client := env.oidcLogin(env.providers[0], freshnessUser(), oidcOrigin+"/")
		require.Equal(t, http.StatusOK, env.send(client, oidcOrigin+"/").StatusCode)
		assertRefused(t, env.post(endpoint, "Authorization", "Bearer "+env.lastAppRequest().Header.Get(apptoken.HeaderName)))
	})
	t.Run("another app", func(t *testing.T) {
		t.Parallel()
		assertRefused(t, env.post("https://public-123.hub.keboola.local"+apptoken.RefreshPath, "Authorization", "Bearer "+refresh))
	})
	t.Run("method", func(t *testing.T) {
		t.Parallel()
		resp := env.get(oidcHost, apptoken.RefreshPath, nil, map[string]string{"Authorization": "Bearer " + refresh})
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
		assert.Equal(t, http.MethodPost, resp.Header.Get("Allow"))
		assert.NotContains(t, readBody(t, resp), "token\"")
	})
}

func TestAppUserTokenRefresh_EndpointOff(t *testing.T) {
	t.Parallel()
	env := startPreviewEnv(t)

	assert.Equal(t, http.StatusNotFound, env.post(oidcOrigin+apptoken.RefreshPath, "Authorization", "Bearer x").StatusCode)
	assert.Empty(t, env.websocketHandshake().Header.Values(apptoken.RefreshHeaderName))
}
