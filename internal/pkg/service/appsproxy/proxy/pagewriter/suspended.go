package pagewriter

import (
	"net/http"
	"time"
)

// suspendedMessage is shown to a data-app frontend that keeps polling a
// background health endpoint (e.g. Streamlit /_stcore/health) after the app
// has been auto-suspended for inactivity. apps-proxy deliberately does NOT
// auto-restart the app from these background polls — doing so would defeat
// auto-suspend for forgotten tabs, which keep polling regardless of whether a
// user is present. The user must reload the page to start the app again
// (a reload hits GET / which is treated as real activity and triggers wakeup).
//
// The message is plain text on purpose: data-app frontends (Streamlit) display
// the health-check response body in a modal that does not render HTML — the
// same reason the spinner/restart messages are plain text for these paths.
// See IsStreamlitHealthCheck and AJDA-1935.
// Kept short on purpose: data-app frontends (Streamlit) show this in a
// connection modal that wraps text but has limited height, so a long message
// ends up behind a scrollbar. The modal renders the body as plain text — it
// does not honor newlines or HTML, so wording is the only lever for length.
const suspendedMessage = "App paused. Reload the page to restart."

// suspendedRetryAfter is advisory back-off for the polling client. It does not
// cause the app to restart — only a user-initiated reload does.
const suspendedRetryAfter = 60 * time.Second

// WriteSuspendedPage responds to a data-app frontend background poll for an
// auto-suspended app. It returns 503 with a plain-text body instructing the
// user to reload, and does NOT trigger a wakeup. See suspendedMessage for the
// rationale (plain text, no auto-restart from background polls).
func (pw *Writer) WriteSuspendedPage(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	w.Header().Set("Retry-After", pw.clock.Now().Add(suspendedRetryAfter).UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(suspendedMessage))
}
