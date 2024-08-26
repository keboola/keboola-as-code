package keboola

import (
	"time"
)

// Config of the Keboolasink bridge.
type Config struct {
	// EventSendTimeout is a timeout to perform slice upload event or file import event.
	EventSendTimeout time.Duration `configKey:"eventSendTimeout" configUsage:"Timeout to perform upload send event of slice or import event of file"`
}

func NewConfig() Config {
	return Config{
		EventSendTimeout: 30 * time.Second,
	}
}
