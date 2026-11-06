package oauthproxy

import (
	"testing"
	"time"

	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/stretchr/testify/assert"
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
		expected time.Time
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
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-10 * time.Minute)},
			expected: now.Add(50*time.Minute + refreshGrace),
			ok:       true,
		},
		{
			name:     "refresh token, refresh overdue: retry one refresh period later",
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-2 * time.Hour)},
			expected: now.Add(time.Hour),
			ok:       true,
		},
		{
			name:     "refresh token, refresh overdue and cookie expiring sooner",
			session:  &sessionsapi.SessionState{RefreshToken: "rt", CreatedAt: at(-(23*time.Hour + 30*time.Minute))},
			expected: now.Add(30 * time.Minute),
			ok:       true,
		},
		{
			name:     "no refresh token: cookie expiry",
			session:  &sessionsapi.SessionState{CreatedAt: at(-10 * time.Minute)},
			expected: now.Add(24*time.Hour - 10*time.Minute),
			ok:       true,
		},
		{
			name:     "no refresh token, cookie about to expire",
			session:  &sessionsapi.SessionState{CreatedAt: at(-(24*time.Hour - 2*time.Second))},
			expected: now.Add(2 * time.Second),
			ok:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deadline, ok := websocketDeadline(tc.session, now)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.expected, deadline)
		})
	}
}
