package etcdclient

import (
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DefaultConnectionTimeout = 30 * time.Second
	DefaultKeepAliveTimeout  = 5 * time.Second
	DefaultKeepAliveInterval = 10 * time.Second
)

type Config struct {
	Endpoint          string        `configKey:"endpoint" configUsage:"Etcd endpoint." validate:"required"`
	Namespace         string        `configKey:"namespace" configUsage:"Etcd namespace." validate:"required"`
	Username          string        `configKey:"username" configUsage:"Etcd username."`
	Password          string        `configKey:"password" configUsage:"Etcd password." sensitive:"true"`
	ConnectTimeout    time.Duration `configKey:"connectTimeout" configUsage:"Etcd connect timeout." validate:"required"`
	KeepAliveTimeout  time.Duration `configKey:"keepAliveTimeout" configUsage:"Etcd keep alive timeout." validate:"required"`
	KeepAliveInterval time.Duration `configKey:"keepAliveInterval" configUsage:"Etcd keep alive interval." validate:"required"`
	DebugLog          bool          `configKey:"debugLog" configUsage:"Etcd operations logging as debug messages."`
}

func NewConfig() Config {
	return Config{
		Endpoint:          "",
		Namespace:         "",
		Username:          "",
		Password:          "",
		DebugLog:          false,
		ConnectTimeout:    DefaultConnectionTimeout,
		KeepAliveTimeout:  DefaultKeepAliveTimeout,
		KeepAliveInterval: DefaultKeepAliveInterval,
	}
}

func (c *Config) Normalize() {
	c.Endpoint = strings.Trim(c.Endpoint, " /")
	c.Namespace = strings.Trim(c.Namespace, " /") + "/"
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("etcd endpoint is not set")
	}
	if c.Namespace == "/" {
		return errors.New("etcd namespace is not set")
	}
	return nil
}
