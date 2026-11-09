package oauthproxy

import (
	"testing"
	"time"

	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/stretchr/testify/assert"

	appupstream "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
)

func TestWebsocketDeadline(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *time.Time {
		v := now.Add(d)
		return &v
	}

	cases := []struct {
		name     string
		session  *sessionsapi.SessionState
		refresh  time.Duration
		expected time.Time
		reason   appupstream.CloseReason
		ok       bool
	}{
		{
			name: "no session",
		},
		{
			name:    "no creation time",
			session: &sessionsapi.SessionState{RefreshToken: "rt"},
		},
		{
			name:     "refresh token: next refresh, just after it is due",
			refresh:  sessionRefresh,
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-10 * time.Minute)},
			expected: now.Add(50*time.Minute + refreshGrace),
			reason:   appupstream.CloseReasonRefreshDue,
			ok:       true,
		},
		{
			name:     "refresh token, refresh overdue: retry one refresh period later",
			refresh:  sessionRefresh,
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-2 * time.Hour)},
			expected: now.Add(time.Hour),
			reason:   appupstream.CloseReasonRefreshDue,
			ok:       true,
		},
		{
			name:     "refresh token, refresh overdue and cookie expiring sooner",
			refresh:  sessionRefresh,
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-(23*time.Hour + 30*time.Minute))},
			expected: now.Add(30 * time.Minute),
			reason:   appupstream.CloseReasonCookieExpiry,
			ok:       true,
		},
		{
			name:     "refresh token, refresh off (GitLab): cookie expiry",
			refresh:  0,
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-10 * time.Minute)},
			expected: now.Add(24*time.Hour - 10*time.Minute),
			reason:   appupstream.CloseReasonCookieExpiry,
			ok:       true,
		},
		{
			name:     "no refresh token: cookie expiry",
			refresh:  sessionRefresh,
			session:  &sessionsapi.SessionState{CreatedAt: at(-10 * time.Minute)},
			expected: now.Add(24*time.Hour - 10*time.Minute),
			reason:   appupstream.CloseReasonCookieExpiry,
			ok:       true,
		},
		{
			name:     "no refresh token, cookie about to expire",
			refresh:  sessionRefresh,
			session:  &sessionsapi.SessionState{CreatedAt: at(-(24*time.Hour - 2*time.Second))},
			expected: now.Add(2 * time.Second),
			reason:   appupstream.CloseReasonCookieExpiry,
			ok:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deadline, reason, ok := websocketDeadline(tc.session, tc.refresh, now)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.expected, deadline)
			assert.Equal(t, tc.reason, reason)
		})
	}
}
