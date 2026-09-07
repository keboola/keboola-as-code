package sessions

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/frameworkpoll"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

const (
	// sweepInterval is how often stale session entries are evicted from memory.
	sweepInterval = 5 * time.Minute

	// websocketGrace is added to the upstream websocket timeout when a session
	// cookie is issued on a handshake, covering the reconnect that follows the
	// connection being dropped, plus any clock skew.
	websocketGrace = 10 * time.Minute
)

// Manager tracks data app sessions and emits their events to Stream.
// When no Stream URL is configured the manager is disabled and every method
// is a no-op, so the feature can be switched off per stack.
type Manager struct {
	enabled   bool
	clock     clockwork.Clock
	logger    log.Logger
	cfg       config.Sessions
	wsTimeout time.Duration
	publicURL *url.URL
	salt      string
	store     *store
	writer    *writer
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	Process() *servicectx.Process
}

func NewManager(ctx context.Context, d dependencies) *Manager {
	cfg := d.Config()
	logger := d.Logger().WithComponent("sessions")

	m := &Manager{
		enabled:   cfg.Sessions.StreamURL != "",
		clock:     d.Clock(),
		logger:    logger,
		cfg:       cfg.Sessions,
		wsTimeout: cfg.Upstream.WsTimeout,
		publicURL: cfg.API.PublicURL,
		salt:      cfg.CookieSecretSalt,
		store:     newStore(),
	}

	if !m.enabled {
		logger.Info(ctx, "session tracking is disabled, no stream url configured")
		return m
	}

	m.writer = newWriter(logger, writerConfig{
		url:         cfg.Sessions.StreamURL,
		queueSize:   cfg.Sessions.QueueSize,
		workers:     cfg.Sessions.Workers,
		sendTimeout: cfg.Sessions.SendTimeout,
	})

	// Evict entries of sessions that went idle. Without this the map grows
	// with every visitor for the lifetime of the process.
	ticker := m.clock.NewTicker(sweepInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				if n := m.store.evictBefore(m.clock.Now().Add(-m.cfg.IdleTimeout)); n > 0 {
					m.logger.Debugf(ctx, "evicted %d idle sessions, %d remaining", n, m.store.len())
				}
			}
		}
	}()

	d.Process().OnShutdown(func(ctx context.Context) {
		m.logger.Info(ctx, "waiting for pending session events")
		m.writer.close(ctx)
	})

	// The websocket timeout is not settable through configuration, so this can
	// only be tripped by a code change — but if it ever is, long connections
	// would be cut short by the cap instead of being covered by it.
	if minimum := m.wsTimeout + websocketGrace; m.cfg.MaxSessionLength < minimum {
		logger.Warnf(ctx,
			"sessions.maxSessionLength (%s) is shorter than the websocket timeout plus grace (%s), long-lived connections will be split across sessions",
			m.cfg.MaxSessionLength, minimum,
		)
	}

	logger.Info(ctx, "session tracking is enabled")
	return m
}

// Session is the per-request view of a session: its identity, and everything
// needed to build an event. Stored in the request context.
type Session struct {
	ID               string
	StartedAt        time.Time
	appID            string
	appName          string
	projectID        string
	authProviderID   string
	authProviderType string
	userEmail        string
	userName         string
	userAgent        string
}

type sessionCtxKey struct{}

func contextWith(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, sessionCtxKey{}, s)
}

// FromContext returns the session of the current request, if tracking is on.
func FromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(sessionCtxKey{}).(*Session)
	return s, ok
}

type authProviderCtxKey struct{}

type authProviderInfo struct {
	id  string
	typ string
}

// WithAuthProvider records which provider admitted the request.
//
// It has to be a separate wrapper because oauth2-proxy does not tell the
// upstream which provider authenticated, and one app can offer several. Each
// per-provider auth handler wraps the shared upstream with this, so the value
// is exact — including for a shared-password app, which never reaches
// oauth2-proxy at all.
func WithAuthProvider(next chain.Handler, id provider.ID, typ provider.Type) chain.Handler {
	info := &authProviderInfo{id: id.String(), typ: string(typ)}
	return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
		ctx := context.WithValue(req.Context(), authProviderCtxKey{}, info)
		return next.ServeHTTPOrError(rw, req.WithContext(ctx))
	})
}

func authProviderFromContext(ctx context.Context) (string, string) {
	if info, ok := ctx.Value(authProviderCtxKey{}).(*authProviderInfo); ok {
		return info.id, info.typ
	}
	return "", ""
}

// Middleware ensures every request carries a session cookie and puts the
// session into the request context.
//
// It is placed between authentication and the upstream so the X-Kbc-User-*
// headers injected by oauth2-proxy are already present, while paths that need
// no authentication still get an anonymous session.
func (m *Manager) Middleware(app api.AppConfig) chain.Middleware {
	if !m.enabled {
		return func(next chain.Handler) chain.Handler { return next }
	}

	key := signingKey(app.ID, m.salt)

	return func(next chain.Handler) chain.Handler {
		return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
			// A frontend background poll is not user activity, so it must not
			// start or extend a session either — otherwise a forgotten browser
			// tab would keep producing sessions for an app nobody is watching.
			if frameworkpoll.Is(req.URL.Path) {
				return next.ServeHTTPOrError(rw, req)
			}
			if s := m.begin(rw, req, app, key); s != nil {
				req = req.WithContext(contextWith(req.Context(), s))
			}
			return next.ServeHTTPOrError(rw, req)
		})
	}
}

// begin resolves or mints the session for this request.
func (m *Manager) begin(rw http.ResponseWriter, req *http.Request, app api.AppConfig, key []byte) *Session {
	ctx := req.Context()
	now := m.clock.Now()

	// The mint time is encoded in the UUIDv7, so it survives a proxy restart,
	// is identical across replicas, and cannot be moved by the client.
	state, found := readCookie(req, key, now, m.cfg.MaxSessionLength)
	if !found {
		sessionID, err := newSessionID()
		if err != nil {
			m.logger.Errorf(ctx, "cannot generate session id: %s", err.Error())
			return nil
		}
		state = cookieState{sessionID: sessionID, startedAt: now}
	}

	// Push the deadline out while the visitor is active. The cookie is written
	// whenever the deadline actually moves, which for an ordinary request means
	// every time: each request has to buy a full idle window, so that a visitor
	// returning inside that window never loses the session no matter how the
	// requests are spaced.
	if deadline := m.deadline(req, state, now); deadline.After(state.deadline) {
		state.deadline = deadline
		setCookie(rw, app, m.publicURL, state.sessionID, deadline, now, key)
	}

	sessionID, startedAt := state.sessionID, state.startedAt

	providerID, providerType := authProviderFromContext(ctx)
	s := &Session{
		ID:               sessionID,
		StartedAt:        startedAt,
		appID:            app.ID.String(),
		appName:          app.Name,
		projectID:        app.ProjectID,
		authProviderID:   providerID,
		authProviderType: providerType,
		userEmail:        req.Header.Get("X-Kbc-User-Email"),
		userName:         req.Header.Get("X-Kbc-User-Name"),
		userAgent:        req.Header.Get("User-Agent"),
	}

	item, _ := m.store.getOrInit(sessionID, now)

	switch {
	case !found:
		// The start of a session is decided by the absence of a valid cookie,
		// never by the absence of cached state. The cache is per-replica and is
		// lost on every restart, so keying this on it would emit a spurious
		// session_start whenever a session moved between replicas. A missing
		// cookie, by contrast, means the id was generated a moment ago and no
		// replica can have seen it.
		m.emit(ctx, s, item, EventSessionStart, "", now)
	case s.userEmail != "" || s.userName != "":
		// A session can start on a public path and authenticate later. Flush a
		// heartbeat as soon as identity appears instead of waiting out the
		// heartbeat interval.
		item.lock.Lock()
		pending := !item.identitySent
		item.lock.Unlock()
		if pending {
			m.emit(ctx, s, item, EventHeartbeat, "", now)
		}
	}

	return s
}

// deadline returns when the session should stop being accepted if nothing more
// is heard from it.
//
// An ordinary request buys IdleTimeout. A websocket handshake buys the whole
// lifetime of that connection instead, because it is the one case where the
// proxy knows the visitor may legitimately be active for hours without sending
// another HTTP request — after the upgrade there is no response left to carry a
// Set-Cookie. This is the same signal that keeps the app from being
// auto-suspended: while frames flow the app stays up, and now the cookie stays
// valid alongside it.
//
// Both are capped at MaxSessionLength from the session's start, so a browser
// left open on a dashboard forever does not report a session measured in weeks.
func (m *Manager) deadline(req *http.Request, state cookieState, now time.Time) time.Time {
	window := m.cfg.IdleTimeout
	if IsWebsocketUpgrade(req) {
		window = m.wsTimeout + websocketGrace
	}

	deadline := now.Add(window)

	// Never move a deadline backwards. An ordinary request arriving during a
	// live websocket — a media fetch, an upload — would otherwise cut the
	// deadline that connection's handshake was granted down to the idle
	// window, and the reconnect hours later would find no valid cookie.
	if state.deadline.After(deadline) {
		deadline = state.deadline
	}

	if limit := state.startedAt.Add(m.cfg.MaxSessionLength); deadline.After(limit) {
		deadline = limit
	}
	return deadline
}

// IsWebsocketUpgrade reports whether the request is a websocket handshake.
//
// It is exported and used by the upstream handler as well, so that the session
// cookie deadline is derived from the very same test that decides whether a
// request is routed onto the long-lived websocket path. If the two definitions
// ever diverged, a connection would outlive the cookie that identifies it.
func IsWebsocketUpgrade(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Connection"), "upgrade") &&
		strings.EqualFold(req.Header.Get("Upgrade"), "websocket")
}

// Activity records that the user made a request to the app.
func (m *Manager) Activity(ctx context.Context) {
	m.activity(ctx, 1, 0)
}

// ActivityWS records one websocket data frame. Streamlit does nearly all of
// its work over a single long-lived websocket, so for those apps this — not
// Activity — is what keeps a session alive.
func (m *Manager) ActivityWS(ctx context.Context) {
	m.activity(ctx, 0, 1)
}

func (m *Manager) activity(ctx context.Context, requests, wsFrames int) {
	if !m.enabled {
		return
	}
	s, ok := FromContext(ctx)
	if !ok {
		return
	}

	now := m.clock.Now()
	item, _ := m.store.getOrInit(s.ID, now)

	item.lock.Lock()
	item.requests += requests
	item.wsFrames += wsFrames
	if now.Before(item.nextHeartbeatAfter) {
		item.lock.Unlock()
		return
	}
	event := m.buildEvent(s, item, EventHeartbeat, "", now)
	item.lock.Unlock()

	m.writer.enqueue(ctx, event)
}

// End records the end of a session. Best-effort by design: a proxy restart
// loses it, so downstream must also apply an idle window.
//
// A ws_close end is not necessarily the end of the visit — Streamlit reconnects
// routinely — so the entry is removed rather than flagged, and later activity
// on the same cookie simply continues the session. One session id can therefore
// carry several session_end rows; downstream takes the last event, not the
// first end. Only a sign-out is final, and that also clears the cookie.
func (m *Manager) End(ctx context.Context, reason EndReason) {
	if !m.enabled {
		return
	}
	s, ok := FromContext(ctx)
	if !ok {
		return
	}

	// No entry means the session was already ended (or never seen on this
	// replica): nothing to report, and this makes a repeated End a no-op.
	item, found := m.store.take(s.ID)
	if !found {
		return
	}

	now := m.clock.Now()

	item.lock.Lock()
	event := m.buildEvent(s, item, EventSessionEnd, reason, now)
	item.lock.Unlock()

	// Detached from the request context: End is called while the connection is
	// being torn down, so the context is already cancelled.
	m.writer.enqueue(context.WithoutCancel(ctx), event)
}

func (m *Manager) emit(ctx context.Context, s *Session, item *entry, typ EventType, reason EndReason, now time.Time) {
	item.lock.Lock()
	event := m.buildEvent(s, item, typ, reason, now)
	item.lock.Unlock()
	m.writer.enqueue(ctx, event)
}

// buildEvent drains the pending deltas and arms the next heartbeat.
// Must be called with item.lock held.
func (m *Manager) buildEvent(s *Session, item *entry, typ EventType, reason EndReason, now time.Time) Event {
	event := Event{
		EventID:          eventID(s.ID, now),
		EventType:        typ,
		EventTime:        formatTime(now),
		SessionID:        s.ID,
		SessionStart:     formatTime(s.StartedAt),
		AppID:            s.appID,
		AppName:          s.appName,
		ProjectID:        s.projectID,
		AuthProviderID:   s.authProviderID,
		AuthProviderType: s.authProviderType,
		UserEmail:        s.userEmail,
		UserName:         s.userName,
		UserAgent:        s.userAgent,
		Requests:         item.requests,
		WSFrames:         item.wsFrames,
		EndReason:        string(reason),
	}

	item.requests = 0
	item.wsFrames = 0
	item.nextHeartbeatAfter = now.Add(m.cfg.HeartbeatInterval)
	if s.userEmail != "" || s.userName != "" {
		item.identitySent = true
	}

	return event
}

// timeFormat has a fixed number of fractional digits, unlike time.RFC3339Nano
// which trims trailing zeros. Without it "…:00Z" and "…:00.5Z" do not order
// correctly as strings, and MIN/MAX over the column in Storage — where these
// are text, not timestamps — would pick the wrong row.
const timeFormat = "2006-01-02T15:04:05.000000Z"

func formatTime(t time.Time) string {
	return t.UTC().Format(timeFormat)
}

// eventID identifies the row. Generation reads crypto/rand and realistically
// cannot fail; if it ever did, fall back to a value derived from the session so
// the column is never empty.
func eventID(sessionID string, at time.Time) string {
	id, err := uuid.NewV7()
	if err != nil {
		return sessionID + "-" + formatTime(at)
	}
	return id.String()
}

// EndRequest ends the session carried by the request cookie and clears that
// cookie, so the next person to use this browser starts a session of their own
// instead of continuing — and being attributed to — this one.
//
// The sign-out path is served by the auth handlers and never reaches the
// upstream, so there is no session in its context and the cookie has to be read
// directly. Identity is omitted: it is already on the start and heartbeat rows
// of the same session.
func (m *Manager) EndRequest(rw http.ResponseWriter, req *http.Request, app api.AppConfig, reason EndReason) {
	if !m.enabled {
		return
	}

	ctx := req.Context()

	state, found := readCookie(req, signingKey(app.ID, m.salt), m.clock.Now(), m.cfg.MaxSessionLength)
	if !found {
		return
	}

	clearCookie(rw, app, m.publicURL)

	s := &Session{
		ID:        state.sessionID,
		StartedAt: state.startedAt,
		appID:     app.ID.String(),
		appName:   app.Name,
		projectID: app.ProjectID,
		userAgent: req.Header.Get("User-Agent"),
	}

	m.End(contextWith(ctx, s), reason)
}
