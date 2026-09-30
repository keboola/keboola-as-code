package preview

import "time"

const (
	SessionIdleTTL       = 4 * time.Hour
	SessionMaxTTL        = 12 * time.Hour
	SessionSlideInterval = 5 * time.Minute
)
