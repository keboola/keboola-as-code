// This file holds the emitting half of the manager: recording activity, and
// turning it into events. sessions.go holds the other half — recognising a
// session and keeping its cookie and its cached state.
package sessions

import (
	"context"
	"net/http"
	"time"

	uuid "github.com/gofrs/uuid/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

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
	event := m.buildEvent(s, item, EventHeartbeat, now)
	item.lock.Unlock()

	m.writer.enqueue(ctx, event)
}

// WebsocketClosed flushes what a connection accumulated, as a heartbeat — not
// an end: Streamlit reconnects routinely and the same cookie carries the session
// on. The context is detached because this runs during teardown, when the
// request context is already cancelled.
func (m *Manager) WebsocketClosed(ctx context.Context) {
	if !m.enabled {
		return
	}
	s, ok := FromContext(ctx)
	if !ok {
		return
	}

	// No entry means this connection was already accounted for, or was never
	// seen on this replica. Either way there is nothing to flush, which also
	// makes a repeated call a no-op.
	item, found := m.store.take(s.ID)
	if !found {
		return
	}

	item.lock.Lock()
	event := m.buildEvent(s, item, EventHeartbeat, m.clock.Now())
	item.lock.Unlock()

	m.writer.enqueue(context.WithoutCancel(ctx), event)
}

// flushAll drains every live session as a heartbeat and empties the store, so a
// deploy does not discard up to one heartbeat interval of activity per session.
// Never an end: the cookie survives a restart, so ending here would split one
// visit into as many sessions as there are deploys.
func (m *Manager) flushAll(ctx context.Context) {
	flushed := 0
	for _, item := range m.store.drainAll() {
		if event, ok := m.drain(item); ok {
			m.writer.enqueue(ctx, event)
			flushed++
		}
	}
	if flushed > 0 {
		m.logger.Infof(ctx, "flushed pending activity of %d session(s)", flushed)
	}
}

// drain turns an entry's pending deltas into a heartbeat. Stamped at lastSeen,
// not now: dating it at shutdown would stretch every open session by however
// long the process stayed up.
func (m *Manager) drain(item *entry) (Event, bool) {
	item.lock.Lock()
	defer item.lock.Unlock()

	if item.session == nil || (item.requests == 0 && item.wsFrames == 0) {
		return Event{}, false
	}
	return m.buildEvent(item.session, item, EventHeartbeat, item.lastSeen), true
}

// buildEvent drains the pending deltas and arms the next heartbeat.
// Must be called with item.lock held.
func (m *Manager) buildEvent(s *Session, item *entry, typ EventType, now time.Time) Event {
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
		ProviderUserID:   s.providerUserID,
		UserAgent:        s.userAgent,
		Requests:         item.requests,
		WSFrames:         item.wsFrames,

		IdleTimeoutSeconds: int(m.cfg.IdleTimeout.Seconds()),
	}

	item.requests = 0
	item.wsFrames = 0
	item.nextHeartbeatAfter = now.Add(m.cfg.HeartbeatInterval)
	if s.providerUserID != "" {
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

// SignOut ends the session and clears the cookie, so the next person to use the
// browser is not attributed to this one. The only emitter of session_end, and
// terminal: the cookie is gone afterwards, which is what makes "at most one
// session_end per session id" an invariant.
//
// Served by the auth handlers, so there is no session in the context and the
// cookie is read directly.
func (m *Manager) SignOut(rw http.ResponseWriter, req *http.Request, app api.AppConfig) {
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

	// Emit even when this replica holds no cached state for the session. The
	// proxy runs several replicas with no session affinity, so a sign-out often
	// lands on one that never saw this session — and it is the only end signal
	// there is, so losing it would leave the session to expire through the idle
	// window instead. Repeating it is not a concern: the cookie was just
	// cleared, so a second sign-out carries none.
	item, found := m.store.take(s.ID)
	if !found {
		item = &entry{}
	}

	item.lock.Lock()
	event := m.buildEvent(s, item, EventSessionEnd, m.clock.Now())
	item.lock.Unlock()

	m.writer.enqueue(context.WithoutCancel(ctx), event)
}
