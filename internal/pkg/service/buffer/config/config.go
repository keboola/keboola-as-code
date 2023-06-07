package config

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix = "BUFFER_API_"
)

// Config is a common config of the Buffer Service.
type Config struct {
	DebugLog           bool          `mapstructure:"debug-log" usage:"Enable debug log level."`
	DebugEtcd          bool          `mapstructure:"debug-etcd" usage:"Enable logging of each etcd KV operation as a debug message."`
	DebugHTTP          bool          `mapstructure:"debug-http" usage:"Log HTTP client request and response bodies."`
	DatadogEnabled     bool          `mapstructure:"datadog-enabled" usage:"Enable Datadog telemetry integration."`
	DatadogDebug       bool          `mapstructure:"datadog-debug" usage:"Enable Datadog debug logs."`
	CPUProfFilePath    string        `mapstructure:"cpu-profile" usage:"Write cpu profile to the file."`
	StorageAPIHost     string        `mapstructure:"storage-api-host" usage:"Host of the Storage API."`
	EtcdEndpoint       string        `mapstructure:"etcd-endpoint" usage:"etcd endpoint."`
	EtcdNamespace      string        `mapstructure:"etcd-namespace" usage:"etcd namespace."`
	EtcdUsername       string        `mapstructure:"etcd-username" usage:"etcd username."`
	EtcdPassword       string        `mapstructure:"etcd-password" usage:"etcd password." sensitive:"true"`
	EtcdConnectTimeout time.Duration `mapstructure:"etcd-connect-timeout" usage:"etcd connect timeout."`
}

type Option func(c *Config)

func NewConfig() Config {
	return Config{
		DebugLog:           false,
		DebugEtcd:          false,
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
		errs.Append(errors.New(`storage API host must be set`))
	}
	if c.EtcdEndpoint == "" {
		errs.Append(errors.New(`etcd endpoint must be set`))
	}
	if c.EtcdNamespace == "" {
		errs.Append(errors.New(`etcd namespace must be set`))
	}
	if c.EtcdConnectTimeout <= 0 {
		errs.Append(errors.Errorf(`etcd connect timeout must be positive time.Duration, found "%v"`, c.EtcdConnectTimeout))
	}
	return errs.ErrorOrNil()
}

func (c Config) Apply(ops ...Option) Config {
	for _, o := range ops {
		o(&c)
	}
	return c
}
