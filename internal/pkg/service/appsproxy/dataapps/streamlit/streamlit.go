// Package streamlit knows the Streamlit frontend's own HTTP traffic, as
// distinct from anything a person did.
//
// It is a package of its own so that both callers share one definition: the
// auto-suspend notification in the upstream handler, and session tracking. The
// helper could equally live in sessions — upstream already imports that — but
// an auto-suspend rule reading out of a package called sessions would be an
// odd place to look for it.
package streamlit

// IsBackgroundPoll reports whether the path is one the Streamlit frontend polls
// on its own, independently of the user, on every websocket (re)connect.
//
// Both callers treat it as non-activity: it must neither keep a running app
// awake nor start a session, or a forgotten browser tab would do both forever.
func IsBackgroundPoll(path string) bool {
	switch path {
	case "/_stcore/health", "/_stcore/host-config":
		return true
	default:
		return false
	}
}
