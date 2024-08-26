package keboola

import (
	"time"
)

// Config of the Keboolasink bridge.
type Config struct {
	// Upload slice event timeout.
	UploadEventSendTimeout time.Duration `configKey:"uploadEventSendTimeout" configUsage:"Timeout to perform upload send event of slice"`
	// Import file event timeout.
	ImportEventSendTimeout time.Duration `configKey:"importEventSendTimeout" configUsage:"Timeout to perform import send event of file"`
}

func NewConfig() Config {
	return Config{
		UploadEventSendTimeout: 30 * time.Second,
		ImportEventSendTimeout: 30 * time.Second,
	}
}
