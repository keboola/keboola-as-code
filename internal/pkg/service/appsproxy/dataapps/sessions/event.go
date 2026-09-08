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
	// EventHeartbeat is emitted periodically while the session is active.
	// It also carries the user id, so a session that starts anonymous and later
	// authenticates gets its identity through the next heartbeat.
	EventHeartbeat EventType = "heartbeat"
	// EventSessionEnd is emitted on a websocket close or an explicit sign-out.
	// It is best-effort: a proxy restart drops it, so downstream must fall back
	// to an idle window over MAX(event_time).
	//
	// A ws_close end is not final — Streamlit reconnects routinely and the same
	// session continues — so one session id can carry several of these. Only a
	// sign-out is final, and it also clears the cookie.
	EventSessionEnd EventType = "session_end"
)

// EndReason explains why the session ended. Only set on EventSessionEnd.
type EndReason string

const (
	EndReasonWebsocketClose EndReason = "ws_close"
	EndReasonSignOut        EndReason = "sign_out"
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

	// UserID is the OIDC subject claim, injected as X-Kbc-User-Id on an
	// authenticated request. It identifies the person without naming them, and
	// is stable across e-mail and display-name changes.
	//
	// Empty for a shared-password app, for a path that requires no
	// authentication, and for GitHub — which issues no ID token, so it has no
	// subject claim to give.
	//
	// Only unique within one issuer: count distinct users over
	// (AuthProviderID, UserID), never UserID alone.
	UserID string `json:"userId"`

	UserAgent string `json:"userAgent"`

	// Requests and WSFrames are deltas since the previous event of this
	// session, not running totals, so they can be summed downstream.
	Requests int `json:"requests"`
	WSFrames int `json:"wsFrames"`

	EndReason string `json:"endReason"`
}
