package bridge

import (
	"time"
)

// Config of the Keboolasink bridge.
type Config struct {
	// Upload event slice timeout.
	UploadEventSendTimeout time.Duration `configKey:"uploadEventSendTimeout" configUsage:"Timeout of uploading send event of slice or file"`
}

func NewConfig() Config {
	return Config{
		UploadEventSendTimeout: 30 * time.Second,
	}
}
