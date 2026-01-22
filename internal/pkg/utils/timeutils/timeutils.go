// Package timeutils provides time formatting utilities.
package timeutils

import (
	"time"

	"github.com/relvacode/iso8601"
)

// FormatISO8601Ptr formats an iso8601.Time pointer to RFC3339 string.
// Returns empty string if the pointer is nil.
func FormatISO8601Ptr(t *iso8601.Time) string {
	if t == nil {
		return ""
	}
	return t.Time.UTC().Format(time.RFC3339)
}
