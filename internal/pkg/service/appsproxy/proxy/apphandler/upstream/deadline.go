package upstream

import (
	"context"
	"time"
)

// CloseReason says why apps-proxy closed a websocket at its deadline.
type CloseReason string

const (
	CloseReasonRefreshDue      CloseReason = "refresh due"
	CloseReasonCookieExpiry    CloseReason = "cookie expiry"
	CloseReasonAppUserTokenCap CloseReason = "app user token cap" //nolint:gosec // G101: a close reason, not a credential
	closeReasonWsTimeout       CloseReason = "ws timeout"
)

// AppUserTokenCap is the longest life of a websocket that carries an app user token, and so of its refresh token.
const AppUserTokenCap = time.Hour

type sessionDeadlineKey struct{}

type appUserTokenKey struct{}

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

// WithAppUserToken marks a request that carries an app user token: a websocket it opens closes within AppUserTokenCap.
func WithAppUserToken(ctx context.Context) context.Context {
	return context.WithValue(ctx, appUserTokenKey{}, true)
}

func sessionDeadlineFrom(ctx context.Context) (sessionDeadline, bool) {
	deadline, ok := ctx.Value(sessionDeadlineKey{}).(sessionDeadline)
	return deadline, ok
}

// WebsocketDeadline is when a websocket opened at the given time is closed: the earliest of
// the session deadline from the context, the websocket timeout and, with an app user token, AppUserTokenCap.
func WebsocketDeadline(ctx context.Context, opened time.Time, wsTimeout time.Duration) (time.Time, CloseReason) {
	at, reason := opened.Add(wsTimeout), closeReasonWsTimeout
	if capped := opened.Add(AppUserTokenCap); ctx.Value(appUserTokenKey{}) != nil && capped.Before(at) {
		at, reason = capped, CloseReasonAppUserTokenCap
	}
	if session, ok := sessionDeadlineFrom(ctx); ok && session.at.Before(at) {
		at, reason = session.at, session.reason
	}
	return at, reason
}
