package upstream

import (
	"context"
	"time"
)

type sessionDeadlineKey struct{}

// WithSessionDeadline sets when a websocket opened by the request must be closed,
// because its authentication session is due for refresh or expires.
func WithSessionDeadline(ctx context.Context, deadline time.Time) context.Context {
	return context.WithValue(ctx, sessionDeadlineKey{}, deadline)
}

func sessionDeadline(ctx context.Context) (time.Time, bool) {
	deadline, ok := ctx.Value(sessionDeadlineKey{}).(time.Time)
	return deadline, ok
}
