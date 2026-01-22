package timeutils

import (
	"testing"
	"time"

	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
)

func TestFormatISO8601Ptr(t *testing.T) {
	t.Parallel()

	// Create a fixed time for testing
	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	iso8601Time := iso8601.Time{Time: fixedTime}

	tests := []struct {
		name     string
		time     *iso8601.Time
		expected string
	}{
		{name: "nil time", time: nil, expected: ""},
		{name: "valid time", time: &iso8601Time, expected: "2024-01-15T10:30:00Z"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatISO8601Ptr(tc.time)
			assert.Equal(t, tc.expected, result)
		})
	}
}
