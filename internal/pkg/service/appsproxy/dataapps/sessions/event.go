// Package sessions tracks end-user sessions in data apps.
//
// A session is identified by a signed cookie minted by the proxy on the first
// request that has none. Session data is emitted as a stream of append-only
// events to a Keboola Stream HTTP source, which lands them in a Storage table.
// Sessions are reconstructed downstream by grouping events on sessionId:
//
//	sessionStart = MIN(event_time), lastActivity = MAX(event_time)
//
// The event model exists because Stream is append-only — there is no way to
// fill in session_end on an already-written row.
package sessions

// EventType identifies what the event records about the session.
type EventType string

const (
	// EventSessionStart is emitted when the proxy mints a new session cookie.
	// Once per session for a browser that keeps cookies; a client that ignores
	// Set-Cookie gets one per request.
	EventSessionStart EventType = "session_start"
	// EventHeartbeat reports activity and drains the counters. Emitted on a
	// throttle interval while the session is active, and additionally whenever
	// something forces a flush: a websocket closing, or the proxy shutting
	// down gracefully. It also carries the user id, so a session that starts
	// anonymous and authenticates later gets its identity through the next one.
	//
	// None of those mean the visit is over. A websocket close is routine —
	// Streamlit reconnects and the same cookie continues the session — and a
	// restart is invisible to the browser.
	EventHeartbeat EventType = "heartbeat"
	// EventSessionEnd means the user signed out. Terminal, and at most one per
	// session id: signing out clears the cookie, so a second sign-out carries
	// none and emits nothing.
	//
	// It is the only end there is. Every other way a visit stops — closing the
	// tab, losing the network, walking away — produces no event at all, so
	// reporting has to treat a session with no end as having stopped
	// idleTimeoutSeconds after its last event.
	EventSessionEnd EventType = "session_end"
)

// Event is one row in the target Storage table. Field names must stay in sync
// with the sink column mapping in scripts/stream-sessions-setup.sh — see
// docs/apps-proxy/sessions.md.
//
// Identity fields are empty when the app has no authentication or uses a shared
// password: those sessions carry a session ID and nothing else.
type Event struct {
	EventID   string    `json:"eventId"`
	EventType EventType `json:"eventType"`
	EventTime string    `json:"eventTime"`

	SessionID    string `json:"sessionId"`
	SessionStart string `json:"sessionStart"`

	AppID     string `json:"appId"`
	AppName   string `json:"appName"`
	ProjectID string `json:"projectId"`

	// AuthProviderID and AuthProviderType identify which configured provider
	// admitted the request. Empty for paths that require no authentication.
	AuthProviderID   string `json:"authProviderId"`
	AuthProviderType string `json:"authProviderType"`

	// ProviderUserID is how the authentication provider names the person: the
	// OIDC subject claim, or the account login for GitHub, which issues no ID
	// token and so has no subject claim to give. Neither is taken from the
	// e-mail claim — though an OIDC issuer is free to use the e-mail address as
	// its subject, so the value can still look like one. That is the issuer's
	// choice; nothing here asks for an address.
	//
	// A subject claim is stable for the life of the account. A GitHub login is
	// not: it can be changed, and a released one can be taken over by another
	// account.
	//
	// Empty for a shared-password app and for a path that requires no
	// authentication.
	//
	// Only unique within one provider: count distinct users over
	// (AuthProviderID, ProviderUserID), never ProviderUserID alone.
	ProviderUserID string `json:"providerUserId"`

	UserAgent string `json:"userAgent"`

	// Requests and WSFrames are deltas since the previous event of this
	// session, not running totals, so they can be summed downstream.
	Requests int `json:"requests"`
	WSFrames int `json:"wsFrames"`

	// IdleTimeoutSeconds is the idle window in force when this event was
	// emitted. A session that did not sign out has no end event, so its end has
	// to be computed as the last event plus this — which makes it part of the
	// data rather than something a query has to know. It is settable per stack,
	// so a query spanning stacks, or one spanning a change to the setting,
	// cannot assume a single value.
	IdleTimeoutSeconds int `json:"idleTimeoutSeconds"`
}
