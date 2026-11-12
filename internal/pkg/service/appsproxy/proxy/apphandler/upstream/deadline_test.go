package upstream_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
)

func TestWebsocketDeadline(t *testing.T) {
	t.Parallel()

	opened := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	wsTimeout := 6 * time.Hour

	t.Run("no session deadline", func(t *testing.T) {
		t.Parallel()
		at, reason := upstream.WebsocketDeadline(t.Context(), opened, wsTimeout)
		assert.Equal(t, opened.Add(wsTimeout), at)
		assert.Equal(t, upstream.CloseReason("ws timeout"), reason)
	})

	t.Run("session deadline first", func(t *testing.T) {
		t.Parallel()
		ctx := upstream.WithSessionDeadline(t.Context(), opened.Add(time.Hour), upstream.CloseReasonRefreshDue)
		at, reason := upstream.WebsocketDeadline(ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(time.Hour), at)
		assert.Equal(t, upstream.CloseReasonRefreshDue, reason)
	})

	t.Run("app user token caps at one hour", func(t *testing.T) {
		t.Parallel()
		ctx := upstream.WithAppUserToken(t.Context())
		at, reason := upstream.WebsocketDeadline(ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(time.Hour), at)
		assert.Equal(t, upstream.CloseReasonAppUserTokenCap, reason)
	})

	t.Run("app user token, session deadline first", func(t *testing.T) {
		t.Parallel()
		ctx := upstream.WithSessionDeadline(t.Context(), opened.Add(30*time.Minute), upstream.CloseReasonRefreshDue)
		ctx = upstream.WithAppUserToken(ctx)
		at, reason := upstream.WebsocketDeadline(ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(30*time.Minute), at)
		assert.Equal(t, upstream.CloseReasonRefreshDue, reason)
	})

	t.Run("app user token, cap before a later session deadline", func(t *testing.T) {
		t.Parallel()
		ctx := upstream.WithSessionDeadline(t.Context(), opened.Add(2*time.Hour), upstream.CloseReasonCookieExpiry)
		ctx = upstream.WithAppUserToken(ctx)
		at, reason := upstream.WebsocketDeadline(ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(time.Hour), at)
		assert.Equal(t, upstream.CloseReasonAppUserTokenCap, reason)
	})

	t.Run("app user token, shorter ws timeout first", func(t *testing.T) {
		t.Parallel()
		at, reason := upstream.WebsocketDeadline(upstream.WithAppUserToken(t.Context()), opened, 10*time.Minute)
		assert.Equal(t, opened.Add(10*time.Minute), at)
		assert.Equal(t, upstream.CloseReason("ws timeout"), reason)
	})

	t.Run("ws timeout first", func(t *testing.T) {
		t.Parallel()
		ctx := upstream.WithSessionDeadline(t.Context(), opened.Add(7*time.Hour), upstream.CloseReasonCookieExpiry)
		at, reason := upstream.WebsocketDeadline(ctx, opened, wsTimeout)
		assert.Equal(t, opened.Add(wsTimeout), at)
		assert.Equal(t, upstream.CloseReason("ws timeout"), reason)
	})
}
