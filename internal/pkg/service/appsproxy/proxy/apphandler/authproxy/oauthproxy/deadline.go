package oauthproxy

import (
	"time"

	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"

	appupstream "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
)

// refreshGrace keeps a websocket open until its session is surely due for refresh:
// oauth2-proxy refreshes only once the session is strictly older than the refresh
// period, measured in whole seconds, and replicas' clocks differ slightly.
const refreshGrace = 5 * time.Second

// websocketDeadline is when a websocket opened with the session must be closed,
// so that the browser reconnects through a refresh or a new login.
func websocketDeadline(session *sessionsapi.SessionState, refresh time.Duration, now time.Time) (time.Time, appupstream.CloseReason, bool) {
	if session == nil || session.CreatedAt == nil || session.CreatedAt.IsZero() {
		return time.Time{}, "", false
	}

	deadline := session.CreatedAt.Add(sessionExpire)
	if session.RefreshToken == "" || refresh == 0 {
		return deadline, appupstream.CloseReasonCookieExpiry, true
	}

	// A refresh that failed without ending the session leaves it overdue; retry it a period later.
	refreshAt := session.CreatedAt.Add(refresh + refreshGrace)
	if !refreshAt.After(now) {
		refreshAt = now.Add(refresh)
	}
	if refreshAt.Before(deadline) {
		return refreshAt, appupstream.CloseReasonRefreshDue, true
	}
	return deadline, appupstream.CloseReasonCookieExpiry, true
}
