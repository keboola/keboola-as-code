package httpsource

import (
	"net/url"
	"time"

	"github.com/c2h5oh/datasize"
)

type Config struct {
	Listen             string            `configKey:"listen" configUsage:"Listen address of the HTTP source." validate:"required,hostname_port"`
	PublicURL          *url.URL          `configKey:"publicUrl" configUsage:"Public URL of the HTTP source for link generation." validate:"required"`
	RequestTimeout     time.Duration     `configKey:"requestTimeout" configUsage:"HTTP request timeout." validate:"required"`
	IdleTimeout        time.Duration     `configKey:"idleTimeout" configUsage:"TCP connection idle timeout." validate:"required"`
	MaxConnections     int               `configKey:"maxConnections" configUsage:"The maximum number of concurrent connections the server may serve." validate:"required"`
	ReadBufferSize     datasize.ByteSize `configKey:"readBufferSize" configUsage:"Read buffer size, all HTTP headers must fit in" validate:"required"`
	WriteBufferSize    datasize.ByteSize `configKey:"writeBufferSize" configUsage:"Write buffer size." validate:"required"`
	PrefetchBodySize   datasize.ByteSize `configKey:"prefetchBodySize" configUsage:"Size of the body loaded to memory before handler." validate:"required"`
	MaxRequestBodySize datasize.ByteSize `configKey:"maxRequestBodySize" configUsage:"Max size of the HTTP request body." validate:"required"`
}

func NewConfig() Config {
	return Config{
		Listen:             "0.0.0.0:7000",
		PublicURL:          nil,
		RequestTimeout:     30 * time.Second,
		IdleTimeout:        30 * time.Second,
		MaxConnections:     200000,
		ReadBufferSize:     16 * datasize.KB,
		WriteBufferSize:    4 * datasize.KB,
		PrefetchBodySize:   64 * datasize.KB,
		MaxRequestBodySize: 1 * datasize.MB,
	}
}
