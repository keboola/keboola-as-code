package config

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix      = "BUFFER_API_"
	SpanNamePrefix = "keboola.go.buffer.task"
)

// Config is a common config of the Buffer Service.
type Config struct {
	Debug              bool          `mapstructure:"debug" usage:"Enable debug log level."`
	DebugHTTP          bool          `mapstructure:"debug-http" usage:"Log HTTP client request and response bodies."`
	DatadogEnabled     bool          `mapstructure:"datadog-enabled" usage:"Enable Datadog telemetry integration."`
	DatadogDebug       bool          `mapstructure:"datadog-debug" usage:"Enable Datadog debug logs."`
	CPUProfFilePath    string        `mapstructure:"cpu-profile" usage:"Write cpu profile to the file."`
	StorageAPIHost     string        `mapstructure:"storage-api-host" usage:"Host of the Storage API."`
	EtcdEndpoint       string        `mapstructure:"etcd-endpoint" usage:"etcd endpoint."`
	EtcdNamespace      string        `mapstructure:"etcd-namespace" usage:"etcd namespace."`
	EtcdUsername       string        `mapstructure:"etcd-username" usage:"etcd username."`
	EtcdPassword       string        `mapstructure:"etcd-password" usage:"etcd password."`
	EtcdConnectTimeout time.Duration `mapstructure:"etcd-connect-timeout" usage:"etcd connect timeout."`
}

type Option func(c *Config)

func NewConfig() Config {
	return Config{
		Debug:              false,
		DebugHTTP:          false,
		CPUProfFilePath:    "",
		DatadogEnabled:     true,
		DatadogDebug:       false,
		StorageAPIHost:     "",
		EtcdEndpoint:       "",
		EtcdNamespace:      "",
		EtcdUsername:       "",
		EtcdPassword:       "",
		EtcdConnectTimeout: 30 * time.Second, // longer default timeout, the etcd could be started at the same time as the API/Worker
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
	c.StorageAPIHost = strhelper.NormalizeHost(c.StorageAPIHost)
}

func (c *Config) Validate() error {
	errs := errors.NewMultiError()
	if c.StorageAPIHost == "" {
		errs.Append(errors.New(`StorageAPIHost must be set`))
	}
	if c.EtcdEndpoint == "" {
		errs.Append(errors.New(`EtcdEndpoint must be set`))
	}
	if c.EtcdNamespace == "" {
		errs.Append(errors.New(`EtcdNamespace must be set`))
	}
	if c.EtcdConnectTimeout <= 0 {
		errs.Append(errors.Errorf(`EtcdConnectTimeout must be positive time.Duration, found "%v"`, c.EtcdConnectTimeout))
	}
	return errs.ErrorOrNil()
}

func (c Config) Apply(ops ...Option) Config {
	for _, o := range ops {
		o(&c)
	}
	return c
}
