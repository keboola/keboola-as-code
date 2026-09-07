// Package frameworkpoll identifies data-app frontend background polls.
//
// It exists as its own package so that "the user did something" has exactly one
// definition. Both the auto-suspend notification (in the upstream handler) and
// session tracking need it, and those two packages already depend on each other
// in the other direction.
package frameworkpoll

// Is reports whether the given URL path is a known data-app frontend
// background-poll endpoint that fires independently of user interaction.
//
// Currently covers Streamlit's /_stcore/health and /_stcore/host-config. These
// are emitted on every WebSocket (re)connect — including the periodic ~20 min
// reconnect cycle imposed by an external idle timeout — and would otherwise
// either bump lastRequestTimestamp on a Running app (defeating auto-suspend) or
// wake a Suspended one (defeating it again). Apps-proxy treats them as
// non-activity: notify is skipped on a Running app and the request is rejected
// with 503 Retry-After on a Suspended one, requiring the user to perform a
// meaningful action (refresh, click into the UI) to wake the app.
//
// They are also not the start of a session: a forgotten browser tab must not
// keep producing sessions for an app nobody is looking at.
func Is(path string) bool {
	switch path {
	case "/_stcore/health", "/_stcore/host-config":
		return true
	default:
		return false
	}
}
