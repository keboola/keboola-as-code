package pagewriter

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyForStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		status        int
		wantTitle     string
		wantRetryable bool
	}{
		{
			name:      "unauthorized",
			status:    http.StatusUnauthorized,
			wantTitle: "You don't have access to this app",
		},
		{
			name:      "forbidden",
			status:    http.StatusForbidden,
			wantTitle: "You don't have access to this app",
		},
		{
			name:      "not found",
			status:    http.StatusNotFound,
			wantTitle: "This app doesn't exist",
		},
		{
			name:          "request timeout",
			status:        http.StatusRequestTimeout,
			wantTitle:     "The app is busy right now",
			wantRetryable: true,
		},
		{
			name:          "too many requests",
			status:        http.StatusTooManyRequests,
			wantTitle:     "The app is busy right now",
			wantRetryable: true,
		},
		{
			name:          "bad gateway",
			status:        http.StatusBadGateway,
			wantTitle:     "This app isn't responding",
			wantRetryable: true,
		},
		{
			name:          "service unavailable",
			status:        http.StatusServiceUnavailable,
			wantTitle:     "This app isn't responding",
			wantRetryable: true,
		},
		{
			name:          "gateway timeout",
			status:        http.StatusGatewayTimeout,
			wantTitle:     "This app isn't responding",
			wantRetryable: true,
		},
		{
			name:          "unmapped server error falls back to the 5xx default",
			status:        http.StatusInternalServerError,
			wantTitle:     "Something went wrong on our side",
			wantRetryable: true,
		},
		{
			name:          "unmapped client error falls back to the 4xx default",
			status:        http.StatusBadRequest,
			wantTitle:     "This request couldn't be completed",
			wantRetryable: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := copyForStatus(tc.status)
			assert.Equal(t, tc.wantTitle, c.Title)
			assert.Equal(t, tc.wantRetryable, c.Retryable)
			// Every status must name a next step: the guidance is the half of the
			// page that tells the user what to do, so an empty one is a bug even
			// when the title happens to read well.
			assert.NotEmpty(t, c.Guidance, "guidance must never be empty")
		})
	}
}

// A retry is only ever offered where repeating the same request could plausibly
// succeed. Offering it on an authorization or addressing failure sends the user
// into a loop that cannot resolve.
func TestCopyForStatus_NoRetryOnPermanentFailures(t *testing.T) {
	t.Parallel()

	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusGone,
	} {
		assert.False(t, copyForStatus(status).Retryable, "status %d must not offer a retry", status)
	}
}

// The plain-text branch is served to Streamlit health checks, which render it as
// text in a modal. It must carry the same explanation as the HTML page.
func TestRenderPlainTextError(t *testing.T) {
	t.Parallel()

	text := renderPlainText("error.gohtml", http.StatusBadGateway)
	require.NotEmpty(t, text)
	assert.Contains(t, text, "This app isn't responding")
	assert.Contains(t, text, "502")
}
