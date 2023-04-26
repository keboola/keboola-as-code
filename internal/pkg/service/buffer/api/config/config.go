package config

import (
	"net/url"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	serviceConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix                         = "BUFFER_API_"
	DefaultStatisticsSyncInterval     = time.Second
	DefaultReceiverBufferSize         = 50 * datasize.MB
	DefaultReceiverBufferSizeCacheTTL = 1 * time.Second
)

// Config of the Buffer API.
type Config struct {
	ServiceConfig              `mapstructure:",squash"`
	PublicAddress              *url.URL          `mapstructure:"public-address" usage:"Public address of the Buffer API, to generate a link URL."`
	ListenAddress              *url.URL          `mapstructure:"listen-address" usage:"API HTTP server listen address."`
	MetricsListenAddress       *url.URL          `mapstructure:"metrics-listen-address" usage:"Prometheus /metrics HTTP endpoint listen address."`
	StatisticsSyncInterval     time.Duration     `mapstructure:"statistics-sync-interval" usage:"Statistics synchronization interval from API node to etcd."`
	ReceiverBufferSize         datasize.ByteSize `mapstructure:"receiver-buffer-size" usage:"Maximum buffered records size in etcd per receiver."`
	ReceiverBufferSizeCacheTTL time.Duration     `mapstructure:"receiver-buffer-size-cache-ttl" usage:"Invalidation interval for receiver buffer size cache."`
}

type ServiceConfig = serviceConfig.Config

type Option func(c *Config)

func NewConfig() Config {
	return Config{
		ServiceConfig:              serviceConfig.NewConfig(),
		ListenAddress:              &url.URL{Scheme: "http", Host: "0.0.0.0:8000"},
		MetricsListenAddress:       &url.URL{Scheme: "http", Host: "0.0.0.0:9000"},
		StatisticsSyncInterval:     DefaultStatisticsSyncInterval,
		ReceiverBufferSize:         DefaultReceiverBufferSize,
		ReceiverBufferSizeCacheTTL: DefaultReceiverBufferSizeCacheTTL,
	}
}

func LoadFrom(args []string, envs env.Provider) (Config, error) {
	cfg := NewConfig()
	err := cfg.LoadFrom(args, envs)
	return cfg, err
}

func (c *Config) LoadFrom(args []string, envs env.Provider) error {
	return cliconfig.LoadTo(c, args, envs, EnvPrefix)
}

func (c *Config) Normalize() {
	c.ServiceConfig.Normalize()
	if c.PublicAddress != nil {
		c.PublicAddress.Host = strhelper.NormalizeHost(c.PublicAddress.Host)
		if c.PublicAddress.Scheme == "" {
			c.PublicAddress.Scheme = "https"
		}
	}
	if c.ListenAddress != nil {
		c.ListenAddress.Host = strhelper.NormalizeHost(c.ListenAddress.Host)
		if c.ListenAddress.Scheme == "" {
			c.ListenAddress.Scheme = "http"
		}
	}
	if c.MetricsListenAddress != nil {
		c.MetricsListenAddress.Host = strhelper.NormalizeHost(c.MetricsListenAddress.Host)
		if c.MetricsListenAddress.Scheme == "" {
			c.MetricsListenAddress.Scheme = "http"
		}
	}
}

func (c *Config) Validate() error {
	errs := errors.NewMultiError()
	if err := c.ServiceConfig.Validate(); err != nil {
		errs.Append(err)
	}
	if c.PublicAddress == nil || c.PublicAddress.String() == "" {
		errs.Append(errors.New("public address is not set"))
	}
	if c.ListenAddress == nil || c.ListenAddress.String() == "" {
		errs.Append(errors.New("listen address is not set"))
	}
	if c.MetricsListenAddress == nil || c.MetricsListenAddress.String() == "" {
		errs.Append(errors.New("metrics listen address is not set"))
	}
	if c.ReceiverBufferSize <= 0 {
		errs.Append(errors.New("receiver buffer size  must be a positive value"))
	}
	if c.ReceiverBufferSizeCacheTTL <= 0 {
		errs.Append(errors.New("receiver buffer size cache TTL must be a positive value"))
	}
	return errs.ErrorOrNil()
}

func (c Config) Apply(ops ...Option) Config {
	for _, o := range ops {
		o(&c)
	}
	return c
}

func WithPublicAddress(v *url.URL) Option {
	return func(c *Config) {
		c.PublicAddress = v
	}
}

func WithListenAddress(v *url.URL) Option {
	return func(c *Config) {
		c.ListenAddress = v
	}
}

// WithStatisticsSyncInterval defines statistics synchronization interval from API node to etcd.
func WithStatisticsSyncInterval(v time.Duration) Option {
	return func(c *Config) {
		c.StatisticsSyncInterval = v
	}
}

// WithReceiverBufferSize defines the maximum receiver buffered records size waiting for upload.
func WithReceiverBufferSize(v datasize.ByteSize) Option {
	return func(c *Config) {
		c.ReceiverBufferSize = v
	}
}

// WithReceiverBufferSizeCacheTTL defines invalidation interval for receiver buffer size cache.
func WithReceiverBufferSizeCacheTTL(v time.Duration) Option {
	return func(c *Config) {
		c.ReceiverBufferSizeCacheTTL = v
	}
}
