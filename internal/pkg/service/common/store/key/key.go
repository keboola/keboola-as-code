package key

import (
	"time"
)

func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}
