// Package sessions tracks end-user sessions in data apps: a signed cookie
// identifies the session, and append-only events go to a Keboola Stream HTTP
// source, which lands them in a Storage table.
//
// See docs/apps-proxy/sessions.md for the data model, the reconstruction query
// and the delivery guarantees.
package sessions

// EventType identifies what the event records about the session.
type EventType string

const (
	// EventSessionStart is emitted when the proxy mints a new session cookie.
	// Once per session for a browser that keeps cookies; a client that ignores
	// Set-Cookie gets one per request.
	EventSessionStart EventType = "session_start"
	// EventHeartbeat reports activity and drains the counters: on the throttle
	// interval, and on any forced flush — a websocket closing, a graceful
	// shutdown. None of those end the visit.
	EventHeartbeat EventType = "heartbeat"
	// EventSessionEnd means the user signed out. The only end there is, and
	// terminal — at most one per session id. Every other way a visit stops
	// emits nothing, so reporting closes those with idleTimeoutSeconds.
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

	// ProviderUserID is how the provider names the person: the OIDC subject
	// claim, or the account login for GitHub, which issues no ID token. Empty
	// for a shared password or no authentication.
	//
	// Only unique within one provider: count distinct users over
	// (AuthProviderID, ProviderUserID), never ProviderUserID alone.
	ProviderUserID string `json:"providerUserId"`

	UserAgent string `json:"userAgent"`

	// Requests and WSFrames are deltas since the previous event of this
	// session, not running totals, so they can be summed downstream.
	Requests int `json:"requests"`
	WSFrames int `json:"wsFrames"`

	// IdleTimeoutSeconds is the idle window in force when the event was
	// emitted. A session that never signed out is closed with it, and it is
	// per-stack settable, so it belongs in the data rather than in the query.
	IdleTimeoutSeconds int `json:"idleTimeoutSeconds"`
}
