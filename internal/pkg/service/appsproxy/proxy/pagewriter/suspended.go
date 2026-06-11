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
// the health-check response body in their connection-error modal, which never
// interprets HTML. Since Streamlit 1.51 the body is wrapped in a Markdown code
// fence (DoInitPings.tsx), so newlines ARE preserved and a multi-line message
// renders as multiple lines; on older Streamlit versions newlines collapse
// into spaces, which degrades gracefully to a single line.
// See IsStreamlitHealthCheck, AJDA-1935 and AJDA-2896.
const suspendedMessage = "App went to sleep due to inactivity. Refresh the page to resume.\n" +
	"To avoid this, the auto-sleep timeout can be increased or disabled in the app settings."

// suspendedRetryAfter is advisory back-off for the polling client. It does not
// cause the app to restart — only a user-initiated reload does.
const suspendedRetryAfter = 60 * time.Second

// WriteSuspendedPage responds to a data-app frontend background poll for an
// auto-suspended app. It returns 503 with a plain-text body instructing the
// user to refresh, and does NOT trigger a wakeup. See suspendedMessage for the
// rationale (plain text, no auto-restart from background polls).
func (pw *Writer) WriteSuspendedPage(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate;")
	w.Header().Set("pragma", "no-cache")
	w.Header().Set("Retry-After", pw.clock.Now().Add(suspendedRetryAfter).UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(suspendedMessage))
}
