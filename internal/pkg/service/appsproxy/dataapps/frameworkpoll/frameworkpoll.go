// Package frameworkpoll identifies data-app frontend background polls, as its
// own package so that "the user did something" has exactly one definition:
// auto-suspend and session tracking both need it, and their packages already
// depend on each other in the other direction.
package frameworkpoll

// Is reports whether the path is a frontend background poll — one that fires
// independently of the user, on every websocket (re)connect.
//
// Treated as non-activity by both callers: it must neither keep a running app
// awake nor start a session, or a forgotten browser tab would do both forever.
func Is(path string) bool {
	switch path {
	case "/_stcore/health", "/_stcore/host-config":
		return true
	default:
		return false
	}
}
