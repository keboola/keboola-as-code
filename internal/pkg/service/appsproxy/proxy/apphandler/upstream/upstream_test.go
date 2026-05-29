package upstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFrameworkBackgroundPoll(t *testing.T) {
	t.Parallel()
	tests := []struct {
		path string
		want bool
	}{
		// Currently filtered Streamlit endpoints.
		{"/_stcore/health", true},
		{"/_stcore/host-config", true},

		// Other Streamlit endpoints — must NOT be filtered.
		{"/_stcore/stream", false},      // WebSocket upgrade — real channel.
		{"/_stcore/upload_file", false}, // User-initiated upload.

		// Real user-driven traffic.
		{"/", false},
		{"/favicon.ico", false},
		{"/apple-touch-icon.png", false},
		{"/_proxy/assets/styles.css", false},

		// Edge cases that must not silently match.
		{"", false},
		{"/_stcore/health/extra", false},
		{"_stcore/health", false}, // no leading slash
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isFrameworkBackgroundPoll(tc.path))
		})
	}
}
