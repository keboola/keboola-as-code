package network

import (
	"time"

	"github.com/c2h5oh/datasize"
)

const (
	TransportProtocolKCP = TransportProtocol("kcp")
	TransportProtocolTCP = TransportProtocol("tcp")
)

type TransportProtocol string

type Config struct {
	Listen             string            `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	Transport          TransportProtocol `configKey:"transport" configUsage:"Transport protocol." validate:"required,oneof=tcp kcp"`
	KeepAliveInterval  time.Duration     `configKey:"keepAliveInterval" configUsage:"Keep alive interval." validate:"required,minDuration=1s,maxDuration=60s"`
	MaxWaitingStreams  int               `configKey:"maxWaitingStreamsPerConn" configUsage:"How many streams may be waiting an accept per connection." validate:"required,min=10,max=100000"`
	StreamMaxWindow    datasize.ByteSize `configKey:"streamMaxWindow" configUsage:"" validate:"required,minBytes=256kB,maxBytes=10MB"`
	StreamOpenTimeout  time.Duration     `configKey:"streamOpenTimeout" configUsage:"Stream ACK timeout." validate:"required,minDuration=1s,maxDuration=30s"`
	StreamCloseTimeout time.Duration     `configKey:"streamCloseTimeout" configUsage:"Stream close timeout." validate:"required,minDuration=1s,maxDuration=30s"`
	StreamWriteTimeout time.Duration     `configKey:"streamWriteTimeout" configUsage:"Stream write timeout." validate:"required,minDuration=1s,maxDuration=60s"`
	ShutdownTimeout    time.Duration     `configKey:"shutdownTimeout" configUsage:"How long the server waits for streams closing." validate:"required,minDuration=1s,max=600s"`
	KCPInputBuffer     datasize.ByteSize `configKey:"kcpInputBuffer" configUsage:"Buffer size for transferring data between source and writer nodes (kcp)." validate:"required,minBytes=16kB,maxBytes=100MB"`
	KCPResponseBuffer  datasize.ByteSize `configKey:"kcpResponseBuffer" configUsage:"Buffer size for transferring responses between writer and source node (kcp)." validate:"required,minBytes=16kB,maxBytes=100MB"`
}

func NewConfig() Config {
	return Config{
		Listen:             "0.0.0.0:6000",
		Transport:          TransportProtocolTCP,
		KeepAliveInterval:  5 * time.Second,
		MaxWaitingStreams:  1024,
		StreamMaxWindow:    8 * datasize.MB,
		StreamOpenTimeout:  10 * time.Second,
		StreamCloseTimeout: 10 * time.Second,
		StreamWriteTimeout: 10 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		KCPInputBuffer:     8 * datasize.MB,
		KCPResponseBuffer:  512 * datasize.KB,
	}
}
