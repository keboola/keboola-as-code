package config

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	serviceConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix = "BUFFER_API_"
)

// Config of the Buffer API.
type Config struct {
	ServiceConfig `mapstructure:",squash"`
	PublicAddress *url.URL `mapstructure:"public-address" usage:"Public address of the Buffer API, to generate a link URL."`
	ListenAddress *url.URL `mapstructure:"listen-address" usage:"Server listen address."`
}

type ServiceConfig = serviceConfig.Config

type Option func(c *Config)

func NewConfig() Config {
	return Config{
		ServiceConfig: serviceConfig.NewConfig(),
		ListenAddress: &url.URL{Scheme: "http", Host: "0.0.0.0:8000"},
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
