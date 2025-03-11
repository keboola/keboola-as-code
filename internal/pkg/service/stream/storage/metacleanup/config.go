package metacleanup

import "time"

type Config struct {
	EnableJobCleanup             bool          `configKey:"enableJobCleanup"  configUsage:"Enable local storage metadata cleanup for jobs."`
	EnableFileCleanup            bool          `configKey:"enableFileCleanup"  configUsage:"Enable local storage metadata cleanup for files."`
	Concurrency                  int           `configKey:"concurrency"  configUsage:"How many files are deleted in parallel." validate:"required,min=1,max=500"`
	ErrorTolerance               int           `configKey:"errorTolerance"  configUsage:"How many errors are tolerated before failing." validate:"required,min=0,max=100"`
	ArchivedFileRetentionPerSink int           `configKey:"archivedFileRetentionPerSink" configMap:"archivedFileRetentionPerSink" configUsage:"Retention period of a file per sink." validate:"required,min=0,max=100"`
	ActiveFileExpiration         time.Duration `configKey:"activeFileExpiration"  configUsage:"Expiration interval of a file that has not yet been imported." validate:"required,minDuration=1h,maxDuration=720h,gtefield=ArchivedFileExpiration"` // maxDuration=30 days
	ArchivedFileExpiration       time.Duration `configKey:"archivedFileExpiration"  configUsage:"Expiration interval of a file that has already been imported." validate:"required,minDuration=15m,maxDuration=720h"`                              // maxDuration=30 days
	FileCleanupInterval          time.Duration `configKey:"fileCleanupInterval"  configUsage:"Cleanup interval of a file." validate:"required,minDuration=30s,maxDuration=24h"`
	JobCleanupInterval           time.Duration `configKey:"jobCleanupInterval"  configUsage:"Cleanup interval of a job that has already completed." validate:"required,minDuration=5s,maxDuration=10m"`
}

func NewConfig() Config {
	return Config{
		EnableFileCleanup:            true,
		EnableJobCleanup:             true,
		Concurrency:                  50,
		ErrorTolerance:               10,
		ArchivedFileRetentionPerSink: 30,
		ActiveFileExpiration:         7 * 24 * time.Hour, // 7 days
		ArchivedFileExpiration:       1 * time.Hour,
		FileCleanupInterval:          10 * time.Minute,
		JobCleanupInterval:           30 * time.Second,
	}
}
