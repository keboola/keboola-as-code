package jobcleanup

import "time"

type Config struct {
	Enable         bool          `configKey:"enable"  configUsage:"Enable local storage metadata cleanup for jobs."`
	Concurrency    int           `configKey:"concurrency"  configUsage:"How many jobs are deleted in parallel." validate:"required,min=1,max=500"`
	ErrorTolerance int           `configKey:"errorTolerance"  configUsage:"How many errors are tolerated before failing." validate:"required,min=0,max=100"`
	Interval       time.Duration `configKey:"interval"  configUsage:"Cleanup interval of a job that has already completed." validate:"required,minDuration=5s,maxDuration=10m"`
}

func NewConfig() Config {
	return Config{
		Enable:         true,
		Concurrency:    50,
		ErrorTolerance: 10,
		Interval:       30 * time.Second,
	}
}
