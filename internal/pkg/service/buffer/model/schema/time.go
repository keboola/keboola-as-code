package schema

import (
	"time"
)

func FormatTimeForKey(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
