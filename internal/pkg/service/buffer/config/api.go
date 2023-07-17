package config

import (
	"net/url"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	APIEnvPrefix                  = "BUFFER_API_"
	DefaultStatisticsSyncInterval = time.Second
	DefaultReceiverBufferSize     = 50 * datasize.MB
)

// APIConfig of the Buffer API.
type APIConfig struct {
	ServiceConfig          `mapstructure:",squash"`
	UniqueID               string            `mapstructure:"unique-id" usage:"Unique process ID, auto-generated by default."`
	PublicAddress          *url.URL          `mapstructure:"public-address" usage:"Public address of the Buffer API, to generate a link URL."`
	ListenAddress          string            `mapstructure:"listen-address" usage:"API HTTP server listen address."`
	MetricsListenAddress   string            `mapstructure:"metrics-listen-address" usage:"Prometheus /metrics HTTP endpoint listen address."`
	StatisticsSyncInterval time.Duration     `mapstructure:"statistics-sync-interval" usage:"Statistics synchronization interval from API node to etcd."`
	ReceiverBufferSize     datasize.ByteSize `mapstructure:"receiver-buffer-size" usage:"Maximum buffered records size in etcd per receiver."`
}

type APIOption func(c *APIConfig)

func NewAPIConfig() APIConfig {
	return APIConfig{
		ServiceConfig:          NewServiceConfig(),
		UniqueID:               "",
		ListenAddress:          "0.0.0.0:8000",
		MetricsListenAddress:   "0.0.0.0:9000",
		StatisticsSyncInterval: DefaultStatisticsSyncInterval,
		ReceiverBufferSize:     DefaultReceiverBufferSize,
	}
}

func BindAPIConfig(args []string, envs env.Provider) (APIConfig, error) {
	cfg := NewAPIConfig()
	err := cfg.LoadFrom(args, envs)
	return cfg, err
}

func (c *APIConfig) LoadFrom(args []string, envs env.Provider) error {
	return cliconfig.LoadTo(c, args, envs, APIEnvPrefix)
}

func (c *APIConfig) Dump() string {
	if kvs, err := cliconfig.Dump(c); err != nil {
		panic(err)
	} else {
		return kvs.String()
	}
}

func (c *APIConfig) Normalize() {
	c.ServiceConfig.Normalize()
	if c.PublicAddress != nil {
		c.PublicAddress.Host = strhelper.NormalizeHost(c.PublicAddress.Host)
		if c.PublicAddress.Scheme == "" {
			c.PublicAddress.Scheme = "https"
		}
	}
}

func (c *APIConfig) Validate() error {
	v := validator.New()
	errs := errors.NewMultiError()
	if err := c.ServiceConfig.Validate(); err != nil {
		errs.Append(err)
	}
	if c.PublicAddress == nil || c.PublicAddress.String() == "" {
		errs.Append(errors.New("public address is not set"))
	}
	if c.ListenAddress == "" {
		errs.Append(errors.New("listen address is not set"))
	} else if err := v.ValidateValue(c.ListenAddress, "hostname_port"); err != nil {
		errs.Append(errors.Errorf(`listen address "%s" is not valid`, c.ListenAddress))
	}
	if c.MetricsListenAddress == "" {
		errs.Append(errors.New("metrics listen address is not set"))
	} else if err := v.ValidateValue(c.MetricsListenAddress, "hostname_port"); err != nil {
		errs.Append(errors.Errorf(`metrics address "%s" is not valid`, c.MetricsListenAddress))
	}
	if c.ReceiverBufferSize <= 0 {
		errs.Append(errors.New("receiver buffer size  must be a positive value"))
	}
	return errs.ErrorOrNil()
}

func (c APIConfig) Apply(ops ...APIOption) APIConfig {
	for _, o := range ops {
		o(&c)
	}
	return c
}

func WithPublicAddress(v *url.URL) APIOption {
	return func(c *APIConfig) {
		c.PublicAddress = v
	}
}

func WithListenAddress(v string) APIOption {
	return func(c *APIConfig) {
		c.ListenAddress = v
	}
}

// WithStatisticsSyncInterval defines statistics synchronization interval from API node to etcd.
func WithStatisticsSyncInterval(v time.Duration) APIOption {
	return func(c *APIConfig) {
		c.StatisticsSyncInterval = v
	}
}

// WithReceiverBufferSize defines the maximum receiver buffered records size waiting for upload.
func WithReceiverBufferSize(v datasize.ByteSize) APIOption {
	return func(c *APIConfig) {
		c.ReceiverBufferSize = v
	}
}
