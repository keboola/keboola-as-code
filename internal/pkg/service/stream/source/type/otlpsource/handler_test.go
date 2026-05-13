package otlpsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractBearerToken(t *testing.T) {
	t.Parallel()

	cases := []struct {
		header string
		want   string
	}{
		{"Bearer abc123", "abc123"},
		{"bearer abc123", "abc123"},      // case-insensitive scheme
		{"BEARER abc123", "abc123"},      // case-insensitive scheme
		{"Bearer  spaced  ", "spaced"},   // outer whitespace trimmed
		{"  Bearer  spaced  ", "spaced"}, // leading whitespace trimmed
		{"Bearer multi-part-token.with.dots", "multi-part-token.with.dots"},
		{"", ""},
		{"abc123", ""},    // no scheme
		{"Basic xyz", ""}, // wrong scheme
		{"Bearer", ""},    // missing token (no space)
		{"Bearer ", ""},   // empty token after scheme
	}
	for _, c := range cases {
		assert.Equal(t, c.want, extractBearerToken(c.header), "input=%q", c.header)
	}
}
