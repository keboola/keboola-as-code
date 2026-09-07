package sessions_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

const eventTimeout = 5 * time.Second

func testApp() api.AppConfig {
	return api.AppConfig{ID: "12345", Name: "my-app", ProjectID: "789"}
}

// streamServer stands in for the Stream HTTP source. The real writer talks
// plain HTTP to it, so this exercises the actual send path.
func streamServer(t *testing.T) (string, <-chan sessions.Event) {
	t.Helper()

	events := make(chan sessions.Event, 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var event sessions.Event
		if !assert.NoError(t, json.NewDecoder(req.Body).Decode(&event)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		events <- event
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	return server.URL, events
}

func newManager(t *testing.T, streamURL string) (*sessions.Manager, *clockwork.FakeClock) {
	t.Helper()

	// The fake clock starts at the real current time: session start is decoded
	// from a UUIDv7 minted off the wall clock, so the two must agree.
	clk := clockwork.NewFakeClockAt(time.Now())

	cfg := config.New()
	cfg.Sessions.StreamURL = streamURL
	publicURL, err := url.Parse("https://hub.keboola.local")
	require.NoError(t, err)
	cfg.API.PublicURL = publicURL

	d, _ := dependencies.NewMockedServiceScope(t, t.Context(), cfg, commonDeps.WithClock(clk))
	return d.SessionsManager(), clk
}

// call runs one request through the sessions middleware. inner runs with the
// session already in its request context.
func call(t *testing.T, m *sessions.Manager, inner func(req *http.Request), cookie *http.Cookie, headers map[string]string) *http.Response {
	t.Helper()

	handler := chain.New(chain.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) error {
		if inner != nil {
			inner(req)
		}
		return nil
	})).Prepend(m.Middleware(testApp()))

	return serve(t, handler, cookie, headers)
}

func serve(t *testing.T, handler chain.Handler, cookie *http.Cookie, headers map[string]string) *http.Response {
	t.Helper()
	return servePath(t, handler, "/", cookie, headers)
}

func websocketHeaders() map[string]string {
	return map[string]string{"Connection": "Upgrade", "Upgrade": "websocket"}
}

func servePath(t *testing.T, handler chain.Handler, path string, cookie *http.Cookie, headers map[string]string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-12345.hub.keboola.local"+path, nil)
	require.NoError(t, err)
	if cookie != nil {
		req.AddCookie(cookie)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	rec := httptest.NewRecorder()
	require.NoError(t, handler.ServeHTTPOrError(rec, req))
	return rec.Result()
}

func sessionCookie(t *testing.T, resp *http.Response) *http.Cookie {
	t.Helper()

	for _, cookie := range resp.Cookies() {
		if cookie.Name == sessions.CookieName {
			return cookie
		}
	}
	t.Fatalf("response did not set the %s cookie", sessions.CookieName)
	return nil
}

// sessionIDFromCookie reads the id out of "<sessionID>.<deadline>.<signature>".
func sessionIDFromCookie(cookie *http.Cookie) string {
	id, _, _ := strings.Cut(cookie.Value, ".")
	return id
}

func hasSessionCookie(resp *http.Response) bool {
	for _, cookie := range resp.Cookies() {
		if cookie.Name == sessions.CookieName {
			return true
		}
	}
	return false
}

func recvEvent(t *testing.T, events <-chan sessions.Event) sessions.Event {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for a session event")
		return sessions.Event{}
	}
}

func expectNoEvent(t *testing.T, events <-chan sessions.Event) {
	t.Helper()

	select {
	case event := <-events:
		t.Fatalf("unexpected session event: %s", event.EventType)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestManager_Disabled(t *testing.T) {
	t.Parallel()

	// No stream URL configured: the whole feature is off, which is how it stays
	// switched off on stacks where Stream is not deployed.
	m, _ := newManager(t, "")

	var sessionFound bool
	resp := call(t, m, func(req *http.Request) {
		_, sessionFound = sessions.FromContext(req.Context())
		m.Activity(req.Context())
		m.End(req.Context(), sessions.EndReasonSignOut)
	}, nil, nil)

	assert.False(t, hasSessionCookie(resp), "no cookie must be set when tracking is disabled")
	assert.False(t, sessionFound)
}

func TestManager_SessionStart(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	resp := call(t, m, nil, nil, nil)

	cookie := sessionCookie(t, resp)
	assert.True(t, cookie.HttpOnly)
	assert.True(t, cookie.Secure)
	// Lax, not Strict: the request landing back on the app after an OAuth
	// redirect is a cross-site top-level navigation. Under Strict the cookie
	// would not be sent and a second session would be minted on every login.
	assert.Equal(t, http.SameSiteLaxMode, cookie.SameSite)
	assert.Equal(t, "12345.hub.keboola.local", cookie.Domain)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionStart, event.EventType)
	assert.Equal(t, "12345", event.AppID)
	assert.Equal(t, "my-app", event.AppName)
	assert.Equal(t, "789", event.ProjectID)
	assert.NotEmpty(t, event.SessionID)
	assert.NotEmpty(t, event.SessionStart)
	assert.Empty(t, event.UserEmail)
	assert.Empty(t, event.UserName)
}

func TestManager_ExistingCookieDoesNotStartNewSession(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// Same cookie, so the same session continues: the cookie is re-issued with
	// a pushed-out deadline, but it carries the same id and emits no second
	// session_start.
	second := call(t, m, nil, cookie, nil)
	require.True(t, hasSessionCookie(second))
	assert.Equal(t, sessionIDFromCookie(cookie), sessionIDFromCookie(sessionCookie(t, second)))
	expectNoEvent(t, events)
}

func TestManager_ForgedCookieStartsNewSession(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	forged := &http.Cookie{Name: sessions.CookieName, Value: "0192f8a1-0000-7000-8000-000000000000.00000000000000000000000000000000"}
	resp := call(t, m, nil, forged, nil)

	// An unsigned session id must not reach the project table.
	minted := sessionCookie(t, resp)
	assert.NotEqual(t, forged.Value, minted.Value)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionStart, event.EventType)
	assert.NotEqual(t, "0192f8a1-0000-7000-8000-000000000000", event.SessionID)
}

func TestManager_Identity(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	headers := map[string]string{
		"X-Kbc-User-Email": "user@example.com",
		"X-Kbc-User-Name":  "Some User",
		"User-Agent":       "test-agent",
	}
	call(t, m, nil, nil, headers)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionStart, event.EventType)
	assert.Equal(t, "user@example.com", event.UserEmail)
	assert.Equal(t, "Some User", event.UserName)
	assert.Equal(t, "test-agent", event.UserAgent)
}

func TestManager_IdentityAppearsLater(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	// Starts anonymous on a public path.
	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)
	require.Empty(t, start.UserEmail)

	// Then the user authenticates. The identity must not wait out the whole
	// heartbeat interval.
	call(t, m, nil, cookie, map[string]string{"X-Kbc-User-Email": "user@example.com"})

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventHeartbeat, event.EventType)
	assert.Equal(t, start.SessionID, event.SessionID)
	assert.Equal(t, "user@example.com", event.UserEmail)

	// Identity is not re-flushed on every following request.
	call(t, m, nil, cookie, map[string]string{"X-Kbc-User-Email": "user@example.com"})
	expectNoEvent(t, events)
}

func TestManager_HeartbeatThrottle(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	activity := func(req *http.Request) { m.Activity(req.Context()) }

	first := call(t, m, activity, nil, nil)
	cookie := sessionCookie(t, first)
	require.Equal(t, sessions.EventSessionStart, recvEvent(t, events).EventType)

	// Within the interval nothing is sent, the request is only counted.
	call(t, m, activity, cookie, nil)
	expectNoEvent(t, events)

	clk.Advance(config.New().Sessions.HeartbeatInterval + time.Second)

	call(t, m, activity, cookie, nil)
	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventHeartbeat, event.EventType)
	// Deltas since the previous event, so they can be summed downstream:
	// three requests were counted and none has been reported yet.
	assert.Equal(t, 3, event.Requests)
	assert.Equal(t, 0, event.WSFrames)
}

func TestManager_ActivityWS(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	require.Equal(t, sessions.EventSessionStart, recvEvent(t, events).EventType)

	// Streamlit does nearly all of its work over one long-lived websocket, so
	// frames — not requests — are what keep those sessions alive.
	//
	// Frames arriving inside the current heartbeat window are only counted.
	call(t, m, func(req *http.Request) {
		m.ActivityWS(req.Context())
		m.ActivityWS(req.Context())
		m.ActivityWS(req.Context())
	}, cookie, nil)
	expectNoEvent(t, events)

	// The first frame past the window flushes everything counted so far, so
	// summing the deltas downstream gives the true frame count.
	clk.Advance(config.New().Sessions.HeartbeatInterval + time.Second)
	call(t, m, func(req *http.Request) { m.ActivityWS(req.Context()) }, cookie, nil)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventHeartbeat, event.EventType)
	assert.Equal(t, 0, event.Requests)
	assert.Equal(t, 4, event.WSFrames)
}

func TestManager_End(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	call(t, m, func(req *http.Request) {
		m.End(req.Context(), sessions.EndReasonWebsocketClose)
		// A second end of the same session must not produce a second row.
		m.End(req.Context(), sessions.EndReasonWebsocketClose)
	}, cookie, nil)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionEnd, event.EventType)
	assert.Equal(t, start.SessionID, event.SessionID)
	assert.Equal(t, string(sessions.EndReasonWebsocketClose), event.EndReason)
	expectNoEvent(t, events)
}

func TestManager_EndRequest(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// The sign-out path never reaches the upstream, so there is no session in
	// its context and the cookie has to be read directly.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-12345.hub.keboola.local/_proxy/sign_out", nil)
	require.NoError(t, err)
	req.AddCookie(cookie)
	m.EndRequest(httptest.NewRecorder(), req, testApp(), sessions.EndReasonSignOut)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionEnd, event.EventType)
	assert.Equal(t, start.SessionID, event.SessionID)
	assert.Equal(t, string(sessions.EndReasonSignOut), event.EndReason)
}

func TestManager_EndRequest_NoCookie(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-12345.hub.keboola.local/_proxy/sign_out", nil)
	require.NoError(t, err)
	m.EndRequest(httptest.NewRecorder(), req, testApp(), sessions.EndReasonSignOut)

	expectNoEvent(t, events)
}

func TestManager_AuthProvider(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	// In production each per-provider auth handler wraps the shared, already
	// session-tracked upstream with this, because oauth2-proxy does not tell
	// the upstream which of several configured providers admitted the request.
	tracked := chain.New(chain.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
		return nil
	})).Prepend(m.Middleware(testApp()))
	stamped := sessions.WithAuthProvider(tracked, provider.ID("company-sso"), provider.TypeOIDC)

	serve(t, stamped, nil, nil)

	event := recvEvent(t, events)
	assert.Equal(t, "company-sso", event.AuthProviderID)
	assert.Equal(t, string(provider.TypeOIDC), event.AuthProviderType)
}

func TestManager_CookieDeadlineFollowsActivity(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)
	idle := config.New().Sessions.IdleTimeout

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	require.Equal(t, sessions.EventSessionStart, recvEvent(t, events).EventType)

	// An ordinary request buys the idle window, not the absolute cap. This is
	// what makes an abandoned tab stop counting in half an hour rather than
	// half a day.
	assert.InDelta(t, idle.Seconds(), float64(cookie.MaxAge), 5)

	// Each further request buys another full window and keeps the same id.
	// The cookie has to be re-issued every time for that: a deadline left in
	// place would quietly still be the previous request's, and a visitor
	// returning inside the window could find it already expired.
	for _, gap := range []time.Duration{time.Minute, idle / 4, idle / 2} {
		clk.Advance(gap)
		resp := call(t, m, nil, cookie, nil)
		require.True(t, hasSessionCookie(resp))

		refreshed := sessionCookie(t, resp)
		assert.Equal(t, sessionIDFromCookie(cookie), sessionIDFromCookie(refreshed))
		assert.InDelta(t, idle.Seconds(), float64(refreshed.MaxAge), 5)
		cookie = refreshed
	}
	expectNoEvent(t, events)
}

func TestManager_HTTPRequestDoesNotShortenAWebsocketDeadline(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	handshake := call(t, m, nil, nil, websocketHeaders())
	cookie := sessionCookie(t, handshake)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// A media fetch or an upload arriving while the connection is live must not
	// cut the deadline that handshake was granted down to the idle window —
	// the reconnect hours later would then find no valid cookie.
	clk.Advance(time.Minute)
	resp := call(t, m, nil, cookie, nil)
	assert.False(t, hasSessionCookie(resp), "the deadline must not move backwards")

	// The connection then runs its full course with no further HTTP traffic,
	// and the reconnect still lands on the same session.
	clk.Advance(config.New().Upstream.WsTimeout)
	call(t, m, func(req *http.Request) { m.Activity(req.Context()) }, cookie, nil)
	assert.Equal(t, start.SessionID, recvEvent(t, events).SessionID)
}

func TestManager_WebsocketHandshakeCoversTheConnection(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	// A handshake is the one case where the visitor may legitimately be active
	// for hours without another HTTP request: after the upgrade there is no
	// response left to carry a Set-Cookie. So that cookie is issued to cover
	// the whole connection — the same signal that keeps the app from being
	// auto-suspended now keeps its session cookie valid.
	resp := call(t, m, nil, nil, websocketHeaders())
	cookie := sessionCookie(t, resp)
	require.Equal(t, sessions.EventSessionStart, recvEvent(t, events).EventType)

	wsTimeout := config.New().Upstream.WsTimeout
	assert.Greater(t, float64(cookie.MaxAge), wsTimeout.Seconds(),
		"the cookie must outlive the connection it was issued for")
	assert.Less(t, float64(cookie.MaxAge), config.New().Sessions.MaxSessionLength.Seconds())
}

func TestManager_SessionSurvivesALongWebsocket(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	// Handshake, then the connection lives for the full upstream timeout with
	// no HTTP traffic at all.
	first := call(t, m, nil, nil, websocketHeaders())
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	clk.Advance(config.New().Upstream.WsTimeout)

	// The reconnect that follows must land on the same session, not mint a new
	// one — otherwise one visit would be reported as several.
	resp := call(t, m, func(req *http.Request) { m.Activity(req.Context()) }, cookie, nil)
	assert.Equal(t, start.SessionID, recvEvent(t, events).SessionID)
	_ = resp
}

func TestManager_IdleSessionExpires(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// Nothing heard for longer than the idle window: the cookie is refused even
	// if the browser still holds it, and the next visit is a new session.
	clk.Advance(config.New().Sessions.IdleTimeout + time.Minute)
	resp := call(t, m, nil, cookie, nil)
	require.True(t, hasSessionCookie(resp))
	assert.NotEqual(t, cookie.Value, sessionCookie(t, resp).Value)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionStart, event.EventType)
	assert.NotEqual(t, start.SessionID, event.SessionID)
}

func TestManager_AbsoluteCapStartsNewSession(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	maxLength := config.New().Sessions.MaxSessionLength
	idle := config.New().Sessions.IdleTimeout
	step := idle / 2

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// Stay continuously active up to one step short of the cap. Requests arrive
	// every half idle window, so the idle timeout never comes close to firing
	// and the session id must not change.
	for range int(maxLength/step) - 1 {
		clk.Advance(step)
		if resp := call(t, m, nil, cookie, nil); hasSessionCookie(resp) {
			cookie = sessionCookie(t, resp)
		}
	}
	expectNoEvent(t, events)

	// The last cookie's deadline was clamped to the cap, so this request is
	// refused even though it is well inside the idle window — without the cap
	// it would still have been valid for another 25 minutes.
	clk.Advance(step + 5*time.Minute)
	resp := call(t, m, nil, cookie, nil)
	require.True(t, hasSessionCookie(resp))
	assert.NotEqual(t, cookie.Value, sessionCookie(t, resp).Value)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventSessionStart, event.EventType)
	assert.NotEqual(t, start.SessionID, event.SessionID,
		"a visit longer than the cap must continue under a new session id")
}

func TestManager_ActivityAfterWebsocketCloseContinuesSession(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// A Streamlit websocket closes and reconnects routinely — on its ~20 min
	// reconnect cycle, on a network blip, at the 6 h upstream timeout — while
	// the user keeps working. Ending the session must not silence it.
	call(t, m, func(req *http.Request) {
		m.End(req.Context(), sessions.EndReasonWebsocketClose)
	}, cookie, nil)
	require.Equal(t, sessions.EventSessionEnd, recvEvent(t, events).EventType)

	clk.Advance(config.New().Sessions.HeartbeatInterval + time.Second)
	call(t, m, func(req *http.Request) { m.Activity(req.Context()) }, cookie, nil)

	event := recvEvent(t, events)
	assert.Equal(t, sessions.EventHeartbeat, event.EventType)
	assert.Equal(t, start.SessionID, event.SessionID, "the same session must continue after a reconnect")
}

func TestManager_SignOutClearsCookie(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://my-app-12345.hub.keboola.local/_proxy/sign_out", nil)
	require.NoError(t, err)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	m.EndRequest(rec, req, testApp(), sessions.EndReasonSignOut)

	require.Equal(t, sessions.EventSessionEnd, recvEvent(t, events).EventType)

	// Without clearing the cookie the next person on this browser would
	// continue — and be attributed to — the session that just signed out.
	cleared := sessionCookie(t, rec.Result())
	assert.Empty(t, cleared.Value)
	assert.Negative(t, cleared.MaxAge)
}

func TestManager_BackgroundPollDoesNotStartSession(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, _ := newManager(t, streamURL)

	// Streamlit emits these on every websocket reconnect, independently of the
	// user. A forgotten browser tab must not keep producing sessions for an app
	// nobody is watching.
	var sessionFound bool
	handler := chain.New(chain.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) error {
		_, sessionFound = sessions.FromContext(req.Context())
		return nil
	})).Prepend(m.Middleware(testApp()))

	for _, path := range []string{"/_stcore/health", "/_stcore/host-config"} {
		resp := servePath(t, handler, path, nil, nil)
		assert.False(t, hasSessionCookie(resp), "%s must not mint a session cookie", path)
		assert.False(t, sessionFound, "%s must not carry a session", path)
	}
	expectNoEvent(t, events)

	// A real request on the same app still starts a session.
	resp := servePath(t, handler, "/", nil, nil)
	assert.True(t, hasSessionCookie(resp))
	assert.Equal(t, sessions.EventSessionStart, recvEvent(t, events).EventType)
}

func TestManager_SlidingExpiration(t *testing.T) {
	t.Parallel()

	streamURL, events := streamServer(t)
	m, clk := newManager(t, streamURL)

	first := call(t, m, nil, nil, nil)
	cookie := sessionCookie(t, first)
	start := recvEvent(t, events)
	require.Equal(t, sessions.EventSessionStart, start.EventType)

	// Every request inside the idle window must buy another full window, so a
	// visitor who keeps coming back within 30 minutes never loses the session
	// whatever the spacing of the requests.
	//
	// 14 then 21 minutes is the case that catches a cookie which is only
	// re-issued past the half-way point: the request at 14 min does not move
	// the deadline, so it silently stays at 30 min, and the request at 35 min
	// is refused even though only 21 minutes have passed since the last one.
	for _, gap := range []time.Duration{14 * time.Minute, 21 * time.Minute} {
		clk.Advance(gap)
		resp := call(t, m, nil, cookie, nil)
		if hasSessionCookie(resp) {
			cookie = sessionCookie(t, resp)
		}
		// A session_start here means the session was lost.
		expectNoEvent(t, events)
	}
}
