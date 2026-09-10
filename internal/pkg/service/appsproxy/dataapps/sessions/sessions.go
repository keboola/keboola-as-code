package sessions

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/frameworkpoll"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	// sweepInterval is how often stale session entries are evicted from memory.
	sweepInterval = 5 * time.Minute

	// userIDHeader carries the provider's user id from oauth2-proxy to this
	// package. It is injected for that purpose and removed again before the
	// request reaches the app, so it is an internal channel rather than part of
	// what a data app sees.
	userIDHeader = "X-Kbc-User-Id"

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
	metrics   *metrics
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	Process() *servicectx.Process
	Telemetry() telemetry.Telemetry
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

	// Without a salt the key is derivable from the app id alone, so every
	// cookie would be forgeable. Config validation should never let this
	// through, but tracking off beats tracking that cannot verify.
	if m.salt == "" {
		m.enabled = false
		logger.Error(ctx, "session tracking is disabled, cookie secret salt is empty")
		return m
	}

	m.metrics = newMetrics(d.Telemetry().Meter(), m.store.len)
	m.writer = newWriter(logger, m.metrics, writerConfig{
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
				if n := m.store.evictBefore(m.clock.Now().Add(-m.retention())); n > 0 {
					m.logger.Debugf(ctx, "evicted %d idle sessions, %d remaining", n, m.store.len())
				}
			}
		}
	}()

	d.Process().OnShutdown(func(ctx context.Context) {
		// Callbacks run LIFO and the HTTP server registers later, so its
		// requests are already drained by the time this runs. Flush first:
		// writer.close only drains the queue, and what every live session
		// counted since its last heartbeat was never queued.
		m.flushAll(ctx)
		m.logger.Info(ctx, "waiting for pending session events")
		m.writer.close(ctx)
	})

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
	providerUserID   string
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

// WithAuthProvider records which provider admitted the request. A separate
// wrapper because oauth2-proxy does not pass that on and one app can offer
// several providers.
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
// session into the request context. It sits between authentication and the
// upstream, so the X-Kbc-User-* headers are already present.
func (m *Manager) Middleware(app api.AppConfig) chain.Middleware {
	var key []byte
	if m.enabled {
		key = signingKey(app.ID, m.salt)
	}

	return func(next chain.Handler) chain.Handler {
		return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
			// A frontend background poll is not user activity, so it must not
			// start or extend a session either — otherwise a forgotten browser
			// tab would keep producing sessions for an app nobody is watching.
			if m.enabled && !frameworkpoll.Is(req.URL.Path) {
				if s := m.begin(rw, req, app, key); s != nil {
					req = req.WithContext(contextWith(req.Context(), s))
				}
			}

			// Strip it here, having read it above. The header exists to carry
			// the claim from oauth2-proxy to this package, and the app has no
			// business seeing a header appear because a stack turned tracking
			// on. The other X-Kbc-User-* headers stay: apps already read them.
			//
			// Unconditional, including when tracking is disabled, so what an
			// app sees does not depend on a per-stack setting.
			req.Header.Del(userIDHeader)

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
		// A page load fires a burst of cookieless requests at once; only the
		// last Set-Cookie survives, so letting each mint a session would
		// report one visit as several.
		if !startsSession(req) {
			return nil
		}

		sessionID, err := newSessionID()
		if err != nil {
			m.logger.Errorf(ctx, "cannot generate session id: %s", err.Error())
			return nil
		}

		// Decode the start back out of the id we just minted rather than using
		// now: every later request derives it that way, and the UUIDv7
		// timestamp is millisecond-truncated, so taking now here would write a
		// different sessionStart on the first row than on all the others.
		startedAt, err := sessionStartFromID(sessionID)
		if err != nil {
			startedAt = now
		}
		state = cookieState{sessionID: sessionID, startedAt: startedAt}
	}

	// Written whenever the deadline actually moves, so a visitor returning
	// inside the idle window never loses the session however the requests are
	// spaced.
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
		providerUserID:   req.Header.Get(userIDHeader),
		userAgent:        req.Header.Get("User-Agent"),
	}

	item, _ := m.store.getOrInit(sessionID, now)

	// Refresh the snapshot on every request, not just the first: a session can
	// start anonymous and authenticate later, and a flush with no request
	// behind it can only report what the entry knows.
	item.lock.Lock()
	item.session = s
	item.lock.Unlock()

	switch {
	case !found:
		// The start of a session is decided by the absence of a valid cookie,
		// never by the absence of cached state. The cache is per-replica and is
		// lost on every restart, so keying this on it would emit a spurious
		// session_start whenever a session moved between replicas. A missing
		// cookie, by contrast, means the id was generated a moment ago and no
		// replica can have seen it.
		item.lock.Lock()
		event := m.buildEvent(s, item, EventSessionStart, now)
		item.lock.Unlock()
		m.writer.enqueue(ctx, event)
	case s.providerUserID != "":
		// A session can start on a public path and authenticate later. Flush a
		// heartbeat as soon as identity appears instead of waiting out the
		// heartbeat interval.
		//
		// The test and the set have to happen in one critical section:
		// buildEvent is what marks the identity as sent, so releasing the lock
		// in between lets two requests arriving together after a login both
		// decide to flush.
		item.lock.Lock()
		var identityEvent *Event
		if !item.identitySent {
			event := m.buildEvent(s, item, EventHeartbeat, now)
			identityEvent = &event
		}
		item.lock.Unlock()

		if identityEvent != nil {
			m.writer.enqueue(ctx, *identityEvent)
		}
	}

	return s
}

// retention is how long an entry is kept after its last activity. It must cover
// the longest deadline a cookie can be given, not just the idle window: lastSeen
// moves only on a data frame, so a live but quiet websocket would otherwise lose
// its entry and its close would flush nothing.
func (m *Manager) retention() time.Duration {
	if ws := m.wsTimeout + websocketGrace; ws > m.cfg.IdleTimeout {
		return ws
	}
	return m.cfg.IdleTimeout
}

// deadline returns when the session stops being accepted if nothing more is
// heard from it: IdleTimeout for an ordinary request, the whole connection for
// a websocket handshake, capped at MaxSessionLength from the session's start.
// See docs/apps-proxy/sessions.md for why the handshake is special.
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

// startsSession reports whether a request without a cookie may mint one. Only
// navigations and websocket handshakes may: a subresource arrives alongside the
// document that triggered it, so letting it mint would turn one visit into
// several.
func startsSession(req *http.Request) bool {
	// Sec-Fetch-Mode distinguishes a navigation from a subresource fetch. It is
	// set by every current browser and cannot be spoofed by page script.
	if mode := req.Header.Get("Sec-Fetch-Mode"); mode != "" {
		return strings.EqualFold(mode, "navigate") || strings.EqualFold(mode, "websocket")
	}

	// Clients that predate Sec-Fetch: asking for HTML is the closest signal
	// that this is a page load rather than something the page pulled in.
	return strings.Contains(req.Header.Get("Accept"), "text/html") || IsWebsocketUpgrade(req)
}

// IsWebsocketUpgrade reports whether the request is a websocket handshake.
// Exported so the upstream routes on the same test the cookie deadline is
// derived from: two definitions would let a connection outlive its cookie.
func IsWebsocketUpgrade(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Connection"), "upgrade") &&
		strings.EqualFold(req.Header.Get("Upgrade"), "websocket")
}
