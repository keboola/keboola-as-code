package distribution

import (
	"time"
)

type Config struct {
	// GrantTimeout it the maximum time to wait for creating a new session.
	GrantTimeout time.Duration `configKey:"grantTimeout" configUsage:"The maximum time to wait for creating a new session." validate:"required,minDuration=1s,maxDuration=1m"`
	// StartupTimeout configures timeout for the node registration to the cluster.
	StartupTimeout time.Duration `configKey:"startupTimeout" configUsage:"Timeout for the node registration to the cluster." validate:"required,minDuration=1s,maxDuration=5m"`
	// ShutdownTimeout configures timeout for the node un-registration from the cluster.
	ShutdownTimeout time.Duration `configKey:"shutdownTimeout" configUsage:"Timeout for the node un-registration from the cluster." validate:"required,minDuration=1s,maxDuration=5m"`
	// EventsGroupInterval configures how often changes in the cluster topology are processed.
	// All changes in the interval are grouped together, so that updates do not occur too often. Use 0 to disable the grouping.
	EventsGroupInterval time.Duration `configKey:"eventsGroupInterval" configUsage:"Interval of processing changes in the topology. Use 0 to disable the grouping." validate:"maxDuration=30s"`
	// TTLSeconds configures the number seconds after which the node is automatically un-registered if an outage occurs.
	TTLSeconds int `configKey:"ttlSeconds" configUsage:"Seconds after which the node is automatically un-registered if an outage occurs." validate:"required,min=1,max=30"`
}

func NewConfig() Config {
	return Config{
		GrantTimeout:        5 * time.Second,
		StartupTimeout:      60 * time.Second,
		ShutdownTimeout:     10 * time.Second,
		EventsGroupInterval: 5 * time.Second,
		TTLSeconds:          15,
	}
}
