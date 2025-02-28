package metacleanup

import "time"

type Config struct {
	Enabled                bool          `configKey:"enabled"  configUsage:"Enable local storage metadata cleanup."`
	Interval               time.Duration `configKey:"interval"  configUsage:"Cleanup interval." validate:"required,minDuration=30s,maxDuration=24h"`
	Concurrency            int           `configKey:"concurrency"  configUsage:"How many files are deleted in parallel." validate:"required,min=1,max=500"`
	ErrorTolerance         int           `configKey:"errorTolerance"  configUsage:"How many errors are tolerated before failing." validate:"required,min=0,max=100"`
	ActiveFileExpiration   time.Duration `configKey:"activeFileExpiration"  configUsage:"Expiration interval of a file that has not yet been imported." validate:"required,minDuration=1h,maxDuration=720h,gtefield=ArchivedFileExpiration"` // maxDuration=30 days
	ArchivedFileExpiration time.Duration `configKey:"archivedFileExpiration"  configUsage:"Expiration interval of a file that has already been imported." validate:"required,minDuration=15m,maxDuration=720h"`                              // maxDuration=30 days
}

func NewConfig() Config {
	return Config{
		Enabled:                true,
		Interval:               30 * time.Second,
		Concurrency:            50,
		ErrorTolerance:         10,
		ActiveFileExpiration:   7 * 24 * time.Hour, // 7 days
		ArchivedFileExpiration: 1 * time.Hour,
	}
}
