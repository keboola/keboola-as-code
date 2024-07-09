package network

import (
	"time"

	"github.com/c2h5oh/datasize"
)

type Config struct {
	Listen             string            `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	KeepAliveInterval  time.Duration     `configKey:"keepAliveInterval" configUsage:"Keep alive interval." validate:"required,minDuration=1s,maxDuration=60s"`
	InputBuffer        datasize.ByteSize `configKey:"inputBuffer" configUsage:"Buffer size for transferring data between source and writer nodes." validate:"required,minBytes=16kB,maxBytes=100MB"`
	ResponseBuffer     datasize.ByteSize `configKey:"responseBuffer" configUsage:"Buffer size for transferring responses between writer and source node." validate:"required,minBytes=16kB,maxBytes=1MB"`
	MaxWaitingStreams  int               `configKey:"maxWaitingStreamsPerConn" configUsage:"How many streams may be waiting an accept per connection." validate:"required,min=10,max=100000"`
	StreamMaxWindow    datasize.ByteSize `configKey:"streamMaxWindow" configUsage:"" validate:"required,minBytes=256kB,maxBytes=10MB"`
	StreamOpenTimeout  time.Duration     `configKey:"streamOpenTimeout" configUsage:"Stream ACK timeout." validate:"required,minDuration=1s,maxDuration=30s"`
	StreamCloseTimeout time.Duration     `configKey:"streamCloseTimeout" configUsage:"Stream close timeout." validate:"required,minDuration=1s,maxDuration=30s"`
	StreamWriteTimeout time.Duration     `configKey:"streamWriteTimeout" configUsage:"Stream write timeout." validate:"required,minDuration=1s,maxDuration=60s"`
	ShutdownTimeout    time.Duration     `configKey:"shutdownTimeout" configUsage:"How long the server waits for streams closing." validate:"required,minDuration=1s,max=600s"`
}

func NewConfig() Config {
	return Config{
		Listen:             "0.0.0.0:6000",
		KeepAliveInterval:  5 * time.Second,
		InputBuffer:        10 * datasize.MB,
		ResponseBuffer:     32 * datasize.KB,
		MaxWaitingStreams:  1024,
		StreamMaxWindow:    512 * datasize.KB,
		StreamOpenTimeout:  5 * time.Second,
		StreamCloseTimeout: 5 * time.Second,
		StreamWriteTimeout: 5 * time.Second,
		ShutdownTimeout:    5 * time.Second,
	}
}
