package diskcleanup

import "time"

type Config struct {
	Enabled     bool          `configKey:"enabled"  configUsage:"Enable local storage disks cleanup."`
	Interval    time.Duration `configKey:"interval"  configUsage:"Cleanup interval." validate:"required,minDuration=5m,maxDuration=24h"`
	Concurrency int           `configKey:"concurrency"  configUsage:"How many directories are removed in parallel." validate:"required,min=1,max=500"`
}

func NewConfig() Config {
	return Config{
		Enabled:     true,
		Interval:    5 * time.Minute,
		Concurrency: 50,
	}
}
