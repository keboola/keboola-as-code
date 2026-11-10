package upstream

import (
	"context"
	"time"
)

// CloseReason says why apps-proxy closed a websocket at its deadline.
type CloseReason string

const (
	CloseReasonRefreshDue   CloseReason = "refresh due"
	CloseReasonCookieExpiry CloseReason = "cookie expiry"
	closeReasonWsTimeout    CloseReason = "ws timeout"
)

type sessionDeadlineKey struct{}

type sessionDeadline struct {
	at     time.Time
	reason CloseReason
}

type deadlineReachedError struct {
	reason CloseReason
}

func (e deadlineReachedError) Error() string {
	return "websocket deadline reached: " + string(e.reason)
}

// WithSessionDeadline sets when a websocket opened by the request must be closed,
// because its authentication session is due for refresh or expires.
func WithSessionDeadline(ctx context.Context, at time.Time, reason CloseReason) context.Context {
	return context.WithValue(ctx, sessionDeadlineKey{}, sessionDeadline{at: at, reason: reason})
}

func sessionDeadlineFrom(ctx context.Context) (sessionDeadline, bool) {
	deadline, ok := ctx.Value(sessionDeadlineKey{}).(sessionDeadline)
	return deadline, ok
}

// WebsocketDeadline is when a websocket opened at the given time is closed: the earlier of
// the session deadline from the context and the websocket timeout.
func WebsocketDeadline(ctx context.Context, opened time.Time, wsTimeout time.Duration) (time.Time, CloseReason) {
	at, reason := opened.Add(wsTimeout), closeReasonWsTimeout
	if session, ok := sessionDeadlineFrom(ctx); ok && session.at.Before(at) {
		at, reason = session.at, session.reason
	}
	return at, reason
}
